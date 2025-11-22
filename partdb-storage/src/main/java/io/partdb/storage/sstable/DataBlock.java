package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

final class DataBlock {

    private static final int TRAILER_SIZE = 8;

    static byte[] serialize(List<Entry> entries) {
        int dataSize = 0;
        for (Entry entry : entries) {
            dataSize += estimateEntrySize(entry);
        }

        ByteBuffer buffer = ByteBuffer.allocate(dataSize + TRAILER_SIZE);

        for (Entry entry : entries) {
            serializeEntry(buffer, entry);
        }

        int entryCount = entries.size();
        byte[] data = new byte[buffer.position()];
        buffer.flip();
        buffer.get(data);

        CRC32 crc = new CRC32();
        crc.update(data);
        int checksum = (int) crc.getValue();

        buffer = ByteBuffer.allocate(data.length + TRAILER_SIZE);
        buffer.put(data);
        buffer.putInt(entryCount);
        buffer.putInt(checksum);

        return buffer.array();
    }

    static List<Entry> deserialize(ByteBuffer buffer) {
        int totalSize = buffer.remaining();
        if (totalSize < TRAILER_SIZE) {
            throw new SSTableException("Block too small");
        }

        int dataSize = totalSize - TRAILER_SIZE;
        byte[] data = new byte[dataSize];
        buffer.get(data);

        int entryCount = buffer.getInt();
        int storedChecksum = buffer.getInt();

        CRC32 crc = new CRC32();
        crc.update(data);
        int computedChecksum = (int) crc.getValue();

        if (storedChecksum != computedChecksum) {
            throw new SSTableException("Block checksum mismatch");
        }

        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        List<Entry> entries = new ArrayList<>(entryCount);

        for (int i = 0; i < entryCount; i++) {
            entries.add(deserializeEntry(dataBuffer));
        }

        return entries;
    }

    private static int estimateEntrySize(Entry entry) {
        int size = 8 + 1 + 8 + 4 + entry.key().size() + 4;
        if (entry.value() != null) {
            size += entry.value().size();
        }
        return size;
    }

    private static void serializeEntry(ByteBuffer buffer, Entry entry) {
        buffer.putLong(entry.timestamp());

        byte flags = 0;
        if (entry.tombstone()) {
            flags |= 0x01;
        }
        buffer.put(flags);

        buffer.putLong(entry.expiresAtMillis());

        byte[] keyBytes = entry.key().toByteArray();
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);

        if (entry.tombstone()) {
            buffer.putInt(0);
        } else {
            byte[] valueBytes = entry.value().toByteArray();
            buffer.putInt(valueBytes.length);
            buffer.put(valueBytes);
        }
    }

    private static Entry deserializeEntry(ByteBuffer buffer) {
        long timestamp = buffer.getLong();
        byte flags = buffer.get();
        boolean tombstone = (flags & 0x01) != 0;
        long expiresAtMillis = buffer.getLong();

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        ByteArray key = ByteArray.wrap(keyBytes);

        int valueLength = buffer.getInt();
        if (tombstone) {
            return new Entry(key, null, timestamp, true, expiresAtMillis);
        } else {
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            ByteArray value = ByteArray.wrap(valueBytes);
            return new Entry(key, value, timestamp, false, expiresAtMillis);
        }
    }
}
