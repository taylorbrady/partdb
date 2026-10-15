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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import java.util.zip.CRC32C;

import static io.partdb.raft.runtime.BinaryFormat.ORDER;

final class SnapshotStore {

    private static final int MAGIC = 0x50414E53;
    private static final int VERSION = 1;
    private static final String PAYLOAD_SUFFIX = ".snap";
    private static final String METADATA_SUFFIX = ".meta";
    private static final String STAGING_SUFFIX = ".tmp";

    record SnapshotFile(SnapshotInfo info, Path payload) {
        SnapshotMetadata metadata() {
            return info.metadata();
        }
    }

    private final Path directory;
    private final Map<Path, Integer> leases = new HashMap<>();
    private final Set<Path> obsolete = new HashSet<>();

    private SnapshotStore(Path directory) {
        this.directory = directory;
    }

    static SnapshotStore open(Path directory) {
        try {
            Files.createDirectories(directory);
            Fsync.directory(directory.getParent());
            try (Stream<Path> files = Files.list(directory)) {
                for (Path file : files.toList()) {
                    String name = file.getFileName().toString();
                    boolean staged = name.endsWith(STAGING_SUFFIX);
                    boolean orphan = name.endsWith(PAYLOAD_SUFFIX) && !Files.exists(metadataFor(file));
                    if (staged || orphan) {
                        Files.delete(file);
                    }
                }
            }
            Fsync.directory(directory);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to open snapshot directory " + directory, e);
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
            throw new RaftStorageException.IO("failed to create snapshot staging file in " + directory, e);
        }
    }

    SnapshotFile install(SnapshotMetadata metadata, Path staged, int checksum) {
        String baseName = "%016x-%016x".formatted(metadata.term(), metadata.index());
        Path payload = directory.resolve(baseName + PAYLOAD_SUFFIX);
        Path metadataFile = directory.resolve(baseName + METADATA_SUFFIX);
        Path metadataStaging = directory.resolve(baseName + METADATA_SUFFIX + STAGING_SUFFIX);

        SnapshotInfo info;
        try {
            try (FileChannel channel = FileChannel.open(staged, StandardOpenOption.WRITE)) {
                channel.force(true);
                info = new SnapshotInfo(metadata, channel.size(), checksum);
            }
            Files.move(staged, payload, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            try (FileChannel channel = FileChannel.open(
                metadataStaging,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )) {
                ByteBuffer encoded = encode(info);
                while (encoded.hasRemaining()) {
                    channel.write(encoded);
                }
                channel.force(true);
            }
            Files.move(metadataStaging, metadataFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Fsync.directory(directory);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to install snapshot " + payload, e);
        }

        deleteOlderThan(metadata.index());
        return new SnapshotFile(info, payload);
    }

    synchronized Lease lease(SnapshotFile file) {
        try {
            var channel = FileChannel.open(file.payload(), StandardOpenOption.READ);
            leases.merge(file.payload(), 1, Integer::sum);
            return new Lease(file, channel);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to open snapshot " + file.payload(), e);
        }
    }

    private synchronized void release(Path payload) {
        int remaining = leases.merge(payload, -1, Integer::sum);
        if (remaining > 0) {
            return;
        }
        leases.remove(payload);
        if (obsolete.remove(payload)) {
            deleteQuietly(payload);
        }
    }

    private synchronized void deleteOlderThan(long index) {
        try {
            boolean deleted = false;
            for (Path metadataFile : metadataFiles()) {
                if (indexOf(metadataFile) >= index) {
                    continue;
                }
                Files.deleteIfExists(metadataFile);
                Path payload = payloadFor(metadataFile);
                if (leases.containsKey(payload)) {
                    obsolete.add(payload);
                } else {
                    Files.deleteIfExists(payload);
                }
                deleted = true;
            }
            if (deleted) {
                Fsync.directory(directory);
            }
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to delete old snapshots in " + directory, e);
        }
    }

    private List<Path> metadataFiles() {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(path -> path.getFileName().toString().endsWith(METADATA_SUFFIX)).toList();
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to list snapshots in " + directory, e);
        }
    }

    private SnapshotFile load(Path metadataFile) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(Files.readAllBytes(metadataFile)).order(ORDER);
            if (buffer.remaining() < Integer.BYTES) {
                throw new RaftStorageException.Corruption("truncated snapshot metadata " + metadataFile);
            }
            int bodyLength = buffer.remaining() - Integer.BYTES;
            CRC32C crc = new CRC32C();
            crc.update(buffer.slice(0, bodyLength));
            if ((int) crc.getValue() != buffer.getInt(bodyLength)) {
                throw new RaftStorageException.Corruption("snapshot metadata checksum mismatch in " + metadataFile);
            }
            if (buffer.getInt() != MAGIC) {
                throw new RaftStorageException.Corruption("invalid snapshot metadata magic in " + metadataFile);
            }
            int version = buffer.getInt();
            if (version != VERSION) {
                throw new RaftStorageException.Corruption("unsupported snapshot metadata version " + version + " in " + metadataFile);
            }
            long term = buffer.getLong();
            long index = buffer.getLong();
            long size = buffer.getLong();
            int checksum = buffer.getInt();
            var membership = BinaryFormat.getMembership(buffer);

            Path payload = payloadFor(metadataFile);
            if (!Files.exists(payload) || Files.size(payload) != size) {
                throw new RaftStorageException.Corruption("snapshot payload missing or truncated: " + payload);
            }
            return new SnapshotFile(new SnapshotInfo(new SnapshotMetadata(index, term, membership), size, checksum), payload);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to load snapshot metadata " + metadataFile, e);
        }
    }

    private static ByteBuffer encode(SnapshotInfo info) {
        var metadata = info.metadata();
        int bodyLength = 4 + 4 + 8 + 8 + 8 + 4 + BinaryFormat.membershipSize(metadata.membership());
        ByteBuffer buffer = ByteBuffer.allocate(bodyLength + Integer.BYTES).order(ORDER);
        buffer.putInt(MAGIC);
        buffer.putInt(VERSION);
        buffer.putLong(metadata.term());
        buffer.putLong(metadata.index());
        buffer.putLong(info.size());
        buffer.putInt(info.checksum());
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

    private static Path metadataFor(Path payload) {
        String name = payload.getFileName().toString();
        return payload.resolveSibling(name.substring(0, name.length() - PAYLOAD_SUFFIX.length()) + METADATA_SUFFIX);
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException _) {
        }
    }

    final class Lease implements RaftTransport.SnapshotSource {
        private final SnapshotFile file;
        private final FileChannel channel;
        private boolean closed;

        private Lease(SnapshotFile file, FileChannel channel) {
            this.file = file;
            this.channel = channel;
        }

        @Override
        public SnapshotInfo info() {
            return file.info();
        }

        @Override
        public FileChannel channel() {
            return channel;
        }

        @Override
        public synchronized void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                channel.close();
            } catch (IOException _) {
            }
            release(file.payload());
        }
    }
}
