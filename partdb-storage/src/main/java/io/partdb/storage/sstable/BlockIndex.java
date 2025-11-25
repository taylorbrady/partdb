package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class BlockIndex {

    public record IndexEntry(ByteArray firstKey, long offset, int size) {}

    private final List<IndexEntry> entries;

    public BlockIndex(List<IndexEntry> entries) {
        this.entries = new ArrayList<>(entries);
    }

    public Optional<IndexEntry> findBlock(ByteArray key) {
        if (entries.isEmpty()) {
            return Optional.empty();
        }

        int left = 0;
        int right = entries.size() - 1;
        IndexEntry candidate = null;

        while (left <= right) {
            int mid = left + (right - left) / 2;
            IndexEntry entry = entries.get(mid);

            int cmp = key.compareTo(entry.firstKey());
            if (cmp >= 0) {
                candidate = entry;
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }

        return Optional.ofNullable(candidate);
    }

    public List<IndexEntry> findBlocksInRange(ByteArray startKey, ByteArray endKey) {
        List<IndexEntry> result = new ArrayList<>();

        int startIndex = 0;
        if (startKey != null) {
            Optional<IndexEntry> startBlock = findBlock(startKey);
            if (startBlock.isEmpty()) {
                return result;
            }
            startIndex = entries.indexOf(startBlock.get());
        }

        for (int i = startIndex; i < entries.size(); i++) {
            IndexEntry entry = entries.get(i);

            if (endKey != null && entry.firstKey().compareTo(endKey) >= 0) {
                break;
            }

            result.add(entry);
        }

        return result;
    }

    public List<IndexEntry> entries() {
        return List.copyOf(entries);
    }

    public int size() {
        return entries.size();
    }

    public byte[] serialize() {
        int totalSize = 0;
        for (IndexEntry entry : entries) {
            totalSize += 4 + entry.firstKey().size() + 8 + 4;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.nativeOrder());
        for (IndexEntry entry : entries) {
            byte[] keyBytes = entry.firstKey().toByteArray();
            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);
            buffer.putLong(entry.offset());
            buffer.putInt(entry.size());
        }

        return buffer.array();
    }

    public static BlockIndex deserialize(MemorySegment segment, int blockCount) {
        List<IndexEntry> entries = new ArrayList<>(blockCount);
        long offset = 0;

        for (int i = 0; i < blockCount; i++) {
            int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            byte[] keyBytes = segment.asSlice(offset + 4, keyLength).toArray(ValueLayout.JAVA_BYTE);
            ByteArray key = ByteArray.wrap(keyBytes);

            long blockOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 4 + keyLength);
            int size = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 4 + keyLength + 8);

            entries.add(new IndexEntry(key, blockOffset, size));
            offset += 4 + keyLength + 8 + 4;
        }

        return new BlockIndex(entries);
    }
}
