package io.partdb.storage.manifest;

import io.partdb.storage.StorageException;
import io.partdb.storage.sstable.SSTableDescriptor;

import java.io.IOException;
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

public record Manifest(
    long nextSSTableId,
    List<SSTableDescriptor> sstables
) {

    private static final int MAGIC_NUMBER = 0x4D414E46;
    private static final int VERSION = 4;
    private static final String MANIFEST_FILENAME = "MANIFEST";
    private static final String MANIFEST_TEMP_FILENAME = "MANIFEST.tmp";

    public Manifest {
        Objects.requireNonNull(sstables, "sstables");
        sstables = List.copyOf(sstables);

        if (nextSSTableId < 0) {
            throw new IllegalArgumentException("nextSSTableId must be non-negative");
        }
    }

    public static Manifest readFrom(Path dataDirectory) {
        try {
            Path manifestPath = dataDirectory.resolve(MANIFEST_FILENAME);

            if (!Files.exists(manifestPath)) {
                return new Manifest(0, List.of());
            }

            byte[] bytes = Files.readAllBytes(manifestPath);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            return fromByteBuffer(buffer);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to read manifest", e);
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
            throw new StorageException.IO("Failed to write manifest", e);
        }
    }

    public List<SSTableDescriptor> level(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .toList();
    }

    public long levelSize(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .mapToLong(SSTableDescriptor::fileSizeBytes)
            .sum();
    }

    public int maxLevel() {
        return sstables.stream()
            .mapToInt(SSTableDescriptor::level)
            .max()
            .orElse(0);
    }

    public byte[] toBytes() {
        int size = calculateSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(MAGIC_NUMBER);
        buffer.putInt(VERSION);
        buffer.putLong(nextSSTableId);
        buffer.putInt(sstables.size());

        for (SSTableDescriptor sst : sstables) {
            sst.writeTo(buffer);
        }

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    public static Manifest fromBytes(byte[] data) {
        return fromByteBuffer(ByteBuffer.wrap(data));
    }

    private static Manifest fromByteBuffer(ByteBuffer buffer) {
        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new StorageException.Corruption("Invalid manifest magic number: " + Integer.toHexString(magic));
        }

        int version = buffer.getInt();
        if (version != VERSION) {
            throw new StorageException.Corruption("Unsupported manifest version: " + version);
        }

        int checksumPosition = buffer.limit() - 4;
        int expectedChecksum = buffer.getInt(checksumPosition);

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, checksumPosition);
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new StorageException.Corruption("Manifest checksum mismatch");
        }

        long nextSSTableId = buffer.getLong();
        int sstableCount = buffer.getInt();

        List<SSTableDescriptor> sstables = new ArrayList<>(sstableCount);
        for (int i = 0; i < sstableCount; i++) {
            sstables.add(SSTableDescriptor.readFrom(buffer));
        }

        return new Manifest(nextSSTableId, sstables);
    }

    private int calculateSize() {
        int size = 4 + 4 + 8 + 4 + 4;
        for (SSTableDescriptor sst : sstables) {
            size += sst.serializedSize();
        }
        return size;
    }
}
