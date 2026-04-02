package io.partdb.storage;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.CRC32C;

record SSTableManifest(
    long nextSSTableId,
    long durableThroughRevision,
    List<SSTableMetadata> sstables
) {

    private static final int MAGIC_NUMBER = 0x4D414E46;
    private static final int VERSION = 5;
    private static final String MANIFEST_FILENAME = "MANIFEST";
    private static final String MANIFEST_TEMP_FILENAME = "MANIFEST.tmp";
    private static final int MIN_SERIALIZED_SIZE = 4 + 4 + 8 + 8 + 4 + 4;

    public SSTableManifest {
        Objects.requireNonNull(sstables, "sstables");
        sstables = List.copyOf(sstables);

        if (nextSSTableId < 0) {
            throw new IllegalArgumentException("nextSSTableId must be non-negative");
        }
        if (durableThroughRevision < 0) {
            throw new IllegalArgumentException("durableThroughRevision must be non-negative");
        }
    }

    public static SSTableManifest readFrom(Path dataDirectory) {
        try {
            Path manifestPath = dataDirectory.resolve(MANIFEST_FILENAME);

            if (!Files.exists(manifestPath)) {
                return new SSTableManifest(0, 0, List.of());
            }

            byte[] bytes = Files.readAllBytes(manifestPath);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            return fromByteBuffer(buffer);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to read SSTable manifest", e);
        }
    }

    public void writeTo(Path dataDirectory) {
        try {
            Path manifestPath = dataDirectory.resolve(MANIFEST_FILENAME);
            Path tempPath = dataDirectory.resolve(MANIFEST_TEMP_FILENAME);

            byte[] serialized = toBytes();

            try (FileChannel channel = FileChannel.open(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

                ByteBuffer buffer = ByteBuffer.wrap(serialized);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }

            Files.move(tempPath, manifestPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to write SSTable manifest", e);
        }
    }

    public List<SSTableMetadata> level(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .toList();
    }

    public long levelSize(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .mapToLong(SSTableMetadata::fileSizeBytes)
            .sum();
    }

    public int maxLevel() {
        return sstables.stream()
            .mapToInt(SSTableMetadata::level)
            .max()
            .orElse(0);
    }

    public byte[] toBytes() {
        int size = calculateSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(MAGIC_NUMBER);
        buffer.putInt(VERSION);
        buffer.putLong(nextSSTableId);
        buffer.putLong(durableThroughRevision);
        buffer.putInt(sstables.size());

        for (SSTableMetadata sst : sstables) {
            sst.writeTo(buffer);
        }

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    public static SSTableManifest fromBytes(byte[] data) {
        return fromByteBuffer(ByteBuffer.wrap(data));
    }

    private static SSTableManifest fromByteBuffer(ByteBuffer buffer) {
        try {
            if (buffer.remaining() < MIN_SERIALIZED_SIZE) {
                throw new StorageException.Corruption("SSTable manifest too small");
            }

            int magic = buffer.getInt();
            if (magic != MAGIC_NUMBER) {
                throw new StorageException.Corruption(
                    "Invalid SSTable manifest magic number: " + Integer.toHexString(magic)
                );
            }

            int version = buffer.getInt();
            if (version != VERSION) {
                throw new StorageException.Corruption("Unsupported SSTable manifest version: " + version);
            }

            int checksumPosition = buffer.limit() - 4;
            int expectedChecksum = buffer.getInt(checksumPosition);

            CRC32C crc = new CRC32C();
            crc.update(buffer.array(), 0, checksumPosition);
            int actualChecksum = (int) crc.getValue();

            if (actualChecksum != expectedChecksum) {
                throw new StorageException.Corruption("SSTable manifest checksum mismatch");
            }

            long nextSSTableId = buffer.getLong();
            long durableThroughRevision = buffer.getLong();
            int sstableCount = buffer.getInt();
            if (sstableCount < 0) {
                throw new StorageException.Corruption("Negative SSTable manifest entry count");
            }

            List<SSTableMetadata> sstables = new ArrayList<>(sstableCount);
            for (int i = 0; i < sstableCount; i++) {
                sstables.add(SSTableMetadata.readFrom(buffer));
            }

            if (buffer.position() != checksumPosition) {
                throw new StorageException.Corruption("Trailing SSTable manifest data");
            }

            return new SSTableManifest(nextSSTableId, durableThroughRevision, sstables);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new StorageException.Corruption("Malformed SSTable manifest", e);
        }
    }

    private int calculateSize() {
        int size = 4 + 4 + 8 + 8 + 4 + 4;
        for (SSTableMetadata sst : sstables) {
            size += sst.serializedSize();
        }
        return size;
    }
}
