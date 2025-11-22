package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public final class Manifest {

    private static final int MAGIC_NUMBER = 0x4D414E46;
    private static final int VERSION = 2;
    private static final String MANIFEST_FILENAME = "MANIFEST";
    private static final String MANIFEST_TEMP_FILENAME = "MANIFEST.tmp";

    public static void write(Path dataDirectory, ManifestData data) {
        try {
            Path manifestPath = dataDirectory.resolve(MANIFEST_FILENAME);
            Path tempPath = dataDirectory.resolve(MANIFEST_TEMP_FILENAME);

            byte[] serialized = serialize(data);

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

    public static ManifestData read(Path dataDirectory) {
        try {
            Path manifestPath = dataDirectory.resolve(MANIFEST_FILENAME);

            if (!Files.exists(manifestPath)) {
                return new ManifestData(0, 0, List.of());
            }

            byte[] bytes = Files.readAllBytes(manifestPath);
            ByteBuffer buffer = ByteBuffer.wrap(bytes);

            return deserialize(buffer);
        } catch (IOException e) {
            throw new ManifestException("Failed to read manifest", e);
        }
    }

    private static byte[] serialize(ManifestData data) {
        int size = calculateSize(data);
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(MAGIC_NUMBER);
        buffer.putInt(VERSION);
        buffer.putLong(data.nextSSTableId());
        buffer.putLong(data.lastAppliedIndex());
        buffer.putInt(data.sstables().size());

        for (SSTableMetadata sst : data.sstables()) {
            buffer.putLong(sst.id());
            buffer.putInt(sst.level());

            buffer.putInt(sst.smallestKey().size());
            buffer.put(sst.smallestKey().toByteArray());

            buffer.putInt(sst.largestKey().size());
            buffer.put(sst.largestKey().toByteArray());

            buffer.putLong(sst.fileSizeBytes());
            buffer.putLong(sst.entryCount());
        }

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    private static ManifestData deserialize(ByteBuffer buffer) {
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

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, checksumPosition);
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new ManifestException("Manifest checksum mismatch");
        }

        long nextSSTableId = buffer.getLong();
        long lastAppliedIndex = buffer.getLong();
        int sstableCount = buffer.getInt();

        List<SSTableMetadata> sstables = new ArrayList<>(sstableCount);
        for (int i = 0; i < sstableCount; i++) {
            long id = buffer.getLong();
            int level = buffer.getInt();

            int smallestKeySize = buffer.getInt();
            byte[] smallestKeyBytes = new byte[smallestKeySize];
            buffer.get(smallestKeyBytes);
            ByteArray smallestKey = ByteArray.wrap(smallestKeyBytes);

            int largestKeySize = buffer.getInt();
            byte[] largestKeyBytes = new byte[largestKeySize];
            buffer.get(largestKeyBytes);
            ByteArray largestKey = ByteArray.wrap(largestKeyBytes);

            long fileSizeBytes = buffer.getLong();
            long entryCount = buffer.getLong();

            sstables.add(new SSTableMetadata(id, level, smallestKey, largestKey, fileSizeBytes, entryCount));
        }

        return new ManifestData(nextSSTableId, lastAppliedIndex, sstables);
    }

    private static int calculateSize(ManifestData data) {
        int size = 4 + 4 + 8 + 8 + 4;

        for (SSTableMetadata sst : data.sstables()) {
            size += 8;
            size += 4;
            size += 4 + sst.smallestKey().size();
            size += 4 + sst.largestKey().size();
            size += 8;
            size += 8;
        }

        size += 4;

        return size;
    }
}
