package io.partdb.raft.runtime;

import io.partdb.raft.SnapshotMetadata;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static io.partdb.raft.runtime.BinaryFormat.ORDER;

final class SnapshotStore {

    private static final int MAGIC = 0x534E4150;
    private static final int VERSION = 2;
    private static final String PAYLOAD_SUFFIX = ".snap";
    private static final String METADATA_SUFFIX = ".meta";
    private static final String STAGING_SUFFIX = ".tmp";

    private final Path directory;

    private SnapshotStore(Path directory) {
        this.directory = directory;
    }

    static SnapshotStore open(Path directory) {
        try {
            Files.createDirectories(directory);
            try (Stream<Path> files = Files.list(directory)) {
                for (Path staged : files.filter(path -> path.getFileName().toString().endsWith(STAGING_SUFFIX)).toList()) {
                    Files.delete(staged);
                }
            }
            Fsync.directory(directory.getParent());
            Fsync.directory(directory);
        } catch (IOException e) {
            throw new RaftStorageException.IO("Failed to open snapshot directory: " + directory, e);
        }
        return new SnapshotStore(directory);
    }

    Optional<SnapshotFile> latest() {
        return metadataFiles().stream()
            .max(Comparator.comparingLong(SnapshotStore::indexOf))
            .map(this::load);
    }

    Path stagingFile() {
        try {
            return Files.createTempFile(directory, "incoming-", STAGING_SUFFIX);
        } catch (IOException e) {
            throw new RaftStorageException.IO("Failed to create snapshot staging file in: " + directory, e);
        }
    }

    SnapshotFile install(SnapshotMetadata metadata, Path staged) {
        String baseName = "%016x-%016x".formatted(metadata.term(), metadata.index());
        Path payload = directory.resolve(baseName + PAYLOAD_SUFFIX);
        Path metadataFile = directory.resolve(baseName + METADATA_SUFFIX);
        Path metadataStaging = directory.resolve(baseName + METADATA_SUFFIX + STAGING_SUFFIX);

        try {
            long size;
            try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.WRITE)) {
                channel.force(true);
                size = channel.size();
            }
            Files.move(staged, payload, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            try (FileChannel channel = FileChannel.open(
                metadataStaging,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
                ByteBuffer encoded = encode(metadata, size);
                while (encoded.hasRemaining()) {
                    channel.write(encoded);
                }
                channel.force(true);
            }
            Files.move(metadataStaging, metadataFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Fsync.directory(directory);
        } catch (IOException e) {
            throw new RaftStorageException.IO("Failed to install snapshot: " + payload, e);
        }

        deleteOlderThan(metadata.index());
        return new SnapshotFile(metadata, payload);
    }

    private void deleteOlderThan(long index) {
        try {
            boolean deleted = false;
            for (Path metadataFile : metadataFiles()) {
                if (indexOf(metadataFile) < index) {
                    Files.deleteIfExists(metadataFile);
                    Files.deleteIfExists(payloadFor(metadataFile));
                    deleted = true;
                }
            }
            if (deleted) {
                Fsync.directory(directory);
            }
        } catch (IOException e) {
            throw new RaftStorageException.IO("Failed to delete old snapshots in: " + directory, e);
        }
    }

    private List<Path> metadataFiles() {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(METADATA_SUFFIX)).toList();
        } catch (IOException e) {
            throw new RaftStorageException.IO("Failed to list snapshots in: " + directory, e);
        }
    }

    private SnapshotFile load(Path metadataFile) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(metadataFile)).order(ORDER);
            if (buffer.remaining() < Integer.BYTES) {
                throw new RaftStorageException.Corruption("Truncated snapshot metadata: " + metadataFile);
            }
            int bodyLength = buffer.remaining() - Integer.BYTES;
            CRC32C crc = new CRC32C();
            crc.update(buffer.slice(0, bodyLength));
            if ((int) crc.getValue() != buffer.getInt(bodyLength)) {
                throw new RaftStorageException.Corruption("Snapshot metadata checksum mismatch: " + metadataFile);
            }
            if (buffer.getInt() != MAGIC) {
                throw new RaftStorageException.Corruption("Invalid snapshot metadata magic: " + metadataFile);
            }
            int version = buffer.getInt();
            if (version != VERSION) {
                throw new RaftStorageException.Corruption("Unsupported snapshot metadata version " + version + ": " + metadataFile);
            }
            long term = buffer.getLong();
            long index = buffer.getLong();
            long size = buffer.getLong();
            var membership = BinaryFormat.getMembership(buffer);

            Path payload = payloadFor(metadataFile);
            if (!Files.exists(payload) || Files.size(payload) != size) {
                throw new RaftStorageException.Corruption("Snapshot payload missing or truncated: " + payload);
            }
            return new SnapshotFile(new SnapshotMetadata(index, term, membership), payload);
        } catch (IOException e) {
            throw new RaftStorageException.IO("Failed to load snapshot metadata: " + metadataFile, e);
        }
    }

    private static ByteBuffer encode(SnapshotMetadata metadata, long payloadSize) {
        int bodyLength = 4 + 4 + 8 + 8 + 8 + BinaryFormat.membershipSize(metadata.membership());
        ByteBuffer buffer = ByteBuffer.allocate(bodyLength + Integer.BYTES).order(ORDER);
        buffer.putInt(MAGIC);
        buffer.putInt(VERSION);
        buffer.putLong(metadata.term());
        buffer.putLong(metadata.index());
        buffer.putLong(payloadSize);
        BinaryFormat.putMembership(buffer, metadata.membership());
        CRC32C crc = new CRC32C();
        crc.update(buffer.slice(0, bodyLength));
        buffer.putInt((int) crc.getValue());
        return buffer.flip();
    }

    private static long indexOf(Path metadataFile) {
        return Long.parseUnsignedLong(metadataFile.getFileName().toString().substring(17, 33), 16);
    }

    private static Path payloadFor(Path metadataFile) {
        String name = metadataFile.getFileName().toString();
        return metadataFile.resolveSibling(name.substring(0, name.length() - METADATA_SUFFIX.length()) + PAYLOAD_SUFFIX);
    }

    record SnapshotFile(SnapshotMetadata metadata, Path payload) {}
}
