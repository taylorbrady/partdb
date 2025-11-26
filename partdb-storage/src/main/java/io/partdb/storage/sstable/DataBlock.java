package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

        ByteBuffer buffer = ByteBuffer.allocate(dataSize + TRAILER_SIZE).order(ByteOrder.nativeOrder());

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

        buffer = ByteBuffer.allocate(data.length + TRAILER_SIZE).order(ByteOrder.nativeOrder());
        buffer.put(data);
        buffer.putInt(entryCount);
        buffer.putInt(checksum);

        return buffer.array();
    }

    static List<Entry> deserialize(MemorySegment segment) {
        long totalSize = segment.byteSize();
        if (totalSize < TRAILER_SIZE) {
            throw new SSTableException("Block too small");
        }

        long dataSize = totalSize - TRAILER_SIZE;

        int entryCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, dataSize);
        int storedChecksum = segment.get(ValueLayout.JAVA_INT_UNALIGNED, dataSize + 4);

        byte[] data = segment.asSlice(0, dataSize).toArray(ValueLayout.JAVA_BYTE);
        CRC32 crc = new CRC32();
        crc.update(data);
        int computedChecksum = (int) crc.getValue();

        if (storedChecksum != computedChecksum) {
            throw new SSTableException("Block checksum mismatch");
        }

        MemorySegment dataSegment = MemorySegment.ofArray(data);
        List<Entry> entries = new ArrayList<>(entryCount);
        long offset = 0;

        for (int i = 0; i < entryCount; i++) {
            long version = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            byte flags = dataSegment.get(ValueLayout.JAVA_BYTE, offset + 8);
            boolean tombstone = (flags & 0x01) != 0;
            long leaseId = dataSegment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 9);
            int keyLength = dataSegment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 17);
            byte[] keyBytes = dataSegment.asSlice(offset + 21, keyLength).toArray(ValueLayout.JAVA_BYTE);
            int valueLength = dataSegment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 21 + keyLength);

            ByteArray key = ByteArray.wrap(keyBytes);
            offset += 21 + keyLength + 4;

            if (tombstone) {
                entries.add(new Entry(key, null, version, true, leaseId));
            } else {
                byte[] valueBytes = dataSegment.asSlice(offset, valueLength).toArray(ValueLayout.JAVA_BYTE);
                offset += valueLength;
                entries.add(new Entry(key, ByteArray.wrap(valueBytes), version, false, leaseId));
            }
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
        buffer.putLong(entry.version());

        byte flags = 0;
        if (entry.tombstone()) {
            flags |= 0x01;
        }
        buffer.put(flags);

        buffer.putLong(entry.leaseId());

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
}
