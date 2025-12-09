package io.partdb.storage.manifest;

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
    List<SSTableInfo> sstables
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

            return deserialize(buffer);
        } catch (IOException e) {
            throw new ManifestException("Failed to read manifest", e);
        }
    }

    public void writeTo(Path dataDirectory) {
        try {
            Path manifestPath = dataDirectory.resolve(MANIFEST_FILENAME);
            Path tempPath = dataDirectory.resolve(MANIFEST_TEMP_FILENAME);

            byte[] serialized = serialize();

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
            throw new ManifestException("Failed to write manifest", e);
        }
    }

    public List<SSTableInfo> level(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .toList();
    }

    public long levelSize(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .mapToLong(SSTableInfo::fileSizeBytes)
            .sum();
    }

    public int maxLevel() {
        return sstables.stream()
            .mapToInt(SSTableInfo::level)
            .max()
            .orElse(0);
    }

    private byte[] serialize() {
        int size = calculateSize();
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(MAGIC_NUMBER);
        buffer.putInt(VERSION);
        buffer.putLong(nextSSTableId);
        buffer.putInt(sstables.size());

        for (SSTableInfo sst : sstables) {
            sst.writeTo(buffer);
        }

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    private static Manifest deserialize(ByteBuffer buffer) {
        int magic = buffer.getInt();
        if (magic != MAGIC_NUMBER) {
            throw new ManifestException("Invalid manifest magic number: " + Integer.toHexString(magic));
        }

        int version = buffer.getInt();
        if (version != VERSION) {
            throw new ManifestException("Unsupported manifest version: " + version);
        }

        int checksumPosition = buffer.limit() - 4;
        int expectedChecksum = buffer.getInt(checksumPosition);

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, checksumPosition);
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new ManifestException("Manifest checksum mismatch");
        }

        long nextSSTableId = buffer.getLong();
        int sstableCount = buffer.getInt();

        List<SSTableInfo> sstables = new ArrayList<>(sstableCount);
        for (int i = 0; i < sstableCount; i++) {
            sstables.add(SSTableInfo.readFrom(buffer));
        }

        return new Manifest(nextSSTableId, sstables);
    }

    private int calculateSize() {
        int size = 4 + 4 + 8 + 4 + 4;
        for (SSTableInfo sst : sstables) {
            size += sst.serializedSize();
        }
        return size;
    }
}
