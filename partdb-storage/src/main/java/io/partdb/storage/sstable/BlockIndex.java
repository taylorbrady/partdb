package io.partdb.storage.sstable;

import io.partdb.common.Slice;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class BlockIndex {

    record Entry(Slice firstKey, BlockHandle handle) {}

    private final List<Entry> entries;

    public BlockIndex(List<Entry> entries) {
        this.entries = List.copyOf(entries);
    }

    public static BlockIndex deserialize(MemorySegment segment, int blockCount) {
        List<Entry> entries = new ArrayList<>(blockCount);
        long offset = 0;

        for (int i = 0; i < blockCount; i++) {
            int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            Slice key = Slice.wrap(segment.asSlice(offset + 4, keyLength));

            long blockOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 4 + keyLength);
            int blockSize = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 4 + keyLength + 8);

            entries.add(new Entry(key, new BlockHandle(blockOffset, blockSize)));
            offset += 4 + keyLength + 8 + 4;
        }

        return new BlockIndex(entries);
    }

    public Optional<Entry> find(Slice key) {
        int index = indexOf(key);
        return index >= 0 ? Optional.of(entries.get(index)) : Optional.empty();
    }

    public List<Entry> findInRange(Slice startKey, Slice endKey) {
        if (entries.isEmpty()) {
            return List.of();
        }

        int startIndex = 0;
        if (startKey != null) {
            startIndex = indexOf(startKey);
            if (startIndex < 0) {
                return List.of();
            }
        }

        List<Entry> result = new ArrayList<>();
        for (int i = startIndex; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            if (endKey != null && entry.firstKey().compareTo(endKey) >= 0) {
                break;
            }
            result.add(entry);
        }

        return result;
    }

    public List<Entry> entries() {
        return entries;
    }

    public int size() {
        return entries.size();
    }

    public byte[] serialize() {
        int totalSize = 0;
        for (Entry entry : entries) {
            totalSize += 4 + entry.firstKey().length() + 8 + 4;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.nativeOrder());
        for (Entry entry : entries) {
            buffer.putInt(entry.firstKey().length());
            buffer.put(entry.firstKey().toByteArray());
            buffer.putLong(entry.handle().offset());
            buffer.putInt(entry.handle().size());
        }

        return buffer.array();
    }

    private int indexOf(Slice key) {
        if (entries.isEmpty()) {
            return -1;
        }

        int low = 0;
        int high = entries.size() - 1;
        int result = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = key.compareTo(entries.get(mid).firstKey());

            if (cmp >= 0) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return result;
    }
}
