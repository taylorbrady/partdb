package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class DataBlockIndex {

    record Entry(Slice firstKey, BlockHandle handle) {}

    private final List<Entry> entries;

    DataBlockIndex(List<Entry> entries) {
        this.entries = List.copyOf(entries);
    }

    static DataBlockIndex deserialize(MemorySegment segment, int blockCount) {
        if (blockCount < 0) {
            throw new StorageException.Corruption("Negative block count: " + blockCount);
        }

        List<Entry> entries = new ArrayList<>(blockCount);
        long offset = 0;
        long size = segment.byteSize();

        for (int i = 0; i < blockCount; i++) {
            if (offset + Integer.BYTES > size) {
                throw new StorageException.Corruption("Truncated data block index key length");
            }
            int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset);
            if (keyLength < 0) {
                throw new StorageException.Corruption("Negative data block index key length");
            }
            long entrySize = Integer.BYTES + keyLength + Long.BYTES + Integer.BYTES;
            if (offset + entrySize > size) {
                throw new StorageException.Corruption("Truncated data block index entry");
            }

            Slice key = Slice.wrap(segment.asSlice(offset + Integer.BYTES, keyLength));
            long blockOffset = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Integer.BYTES + keyLength);
            int blockSize = segment.get(
                ValueLayout.JAVA_INT_UNALIGNED,
                offset + Integer.BYTES + keyLength + Long.BYTES
            );

            entries.add(new Entry(key, new BlockHandle(blockOffset, blockSize)));
            offset += entrySize;
        }

        if (offset != size) {
            throw new StorageException.Corruption("Trailing data block index data");
        }

        return new DataBlockIndex(entries);
    }

    Optional<Entry> find(Slice key) {
        int index = indexOf(key);
        return index >= 0 ? Optional.of(entries.get(index)) : Optional.empty();
    }

    List<Entry> findInRange(ScanBounds bounds) {
        if (entries.isEmpty()) {
            return List.of();
        }

        int startIndex = 0;
        Slice startKey = bounds.startInclusive();
        if (startKey != null) {
            startIndex = indexOf(startKey);
            if (startIndex < 0) {
                startIndex = 0;
            }
        }

        List<Entry> result = new ArrayList<>();
        Slice endKey = bounds.endExclusive();
        for (int i = startIndex; i < entries.size(); i++) {
            Entry entry = entries.get(i);
            if (endKey != null && entry.firstKey().compareTo(endKey) >= 0) {
                break;
            }
            result.add(entry);
        }

        return result;
    }

    List<Entry> entries() {
        return entries;
    }

    byte[] serialize() {
        int totalSize = 0;
        for (Entry entry : entries) {
            totalSize += Integer.BYTES + entry.firstKey().length() + Long.BYTES + Integer.BYTES;
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
            int comparison = key.compareTo(entries.get(mid).firstKey());

            if (comparison >= 0) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return result;
    }
}
