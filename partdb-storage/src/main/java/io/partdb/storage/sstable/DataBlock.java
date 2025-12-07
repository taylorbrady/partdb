package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.storage.Entry;

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

        int dataEnd = buffer.position();

        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, dataEnd);

        buffer.putInt(entries.size());
        buffer.putInt((int) crc.getValue());

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
            byte flags = dataSegment.get(ValueLayout.JAVA_BYTE, offset);
            boolean tombstone = (flags & 0x01) != 0;
            int keyLength = dataSegment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 1);
            byte[] keyBytes = dataSegment.asSlice(offset + 5, keyLength).toArray(ValueLayout.JAVA_BYTE);
            int valueLength = dataSegment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 5 + keyLength);

            ByteArray key = ByteArray.wrap(keyBytes);
            offset += 5 + keyLength + 4;

            if (tombstone) {
                entries.add(new Entry.Tombstone(key));
            } else {
                byte[] valueBytes = dataSegment.asSlice(offset, valueLength).toArray(ValueLayout.JAVA_BYTE);
                offset += valueLength;
                entries.add(new Entry.Data(key, ByteArray.wrap(valueBytes)));
            }
        }

        return entries;
    }

    private static int estimateEntrySize(Entry entry) {
        return switch (entry) {
            case Entry.Data data -> 1 + 4 + entry.key().size() + 4 + data.value().size();
            case Entry.Tombstone _ -> 1 + 4 + entry.key().size() + 4;
        };
    }

    private static void serializeEntry(ByteBuffer buffer, Entry entry) {
        byte[] keyBytes = entry.key().toByteArray();

        switch (entry) {
            case Entry.Tombstone _ -> {
                buffer.put((byte) 0x01);
                buffer.putInt(keyBytes.length);
                buffer.put(keyBytes);
                buffer.putInt(0);
            }
            case Entry.Data data -> {
                buffer.put((byte) 0x00);
                buffer.putInt(keyBytes.length);
                buffer.put(keyBytes);
                byte[] valueBytes = data.value().toByteArray();
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            }
        }
    }
}
