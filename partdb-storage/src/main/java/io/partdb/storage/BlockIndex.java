package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

final class BlockIndex {

    record Handle(Slice firstKey, long offset, int size) {}

    private final List<Handle> handles;

    BlockIndex(List<Handle> handles) {
        this.handles = List.copyOf(handles);
    }

    static BlockIndex decode(MemorySegment segment, int count) {
        Encoding.check(count >= 0, "negative block count: " + count);
        List<Handle> handles = new ArrayList<>(count);
        long offset = 0;
        long size = segment.byteSize();
        for (int i = 0; i < count; i++) {
            Encoding.check(offset + Integer.BYTES <= size, "truncated block index");
            int keyLength = segment.get(Encoding.INT, offset);
            Encoding.check(keyLength >= 0, "negative block index key length");
            long keyOffset = offset + Integer.BYTES;
            Encoding.check(keyOffset + keyLength + Long.BYTES + Integer.BYTES <= size, "truncated block index entry");
            Slice key = Slice.wrap(segment.asSlice(keyOffset, keyLength));
            long blockOffset = segment.get(Encoding.LONG, keyOffset + keyLength);
            int blockSize = segment.get(Encoding.INT, keyOffset + keyLength + Long.BYTES);
            Encoding.check(blockOffset >= 0 && blockSize > 0, "invalid block handle");
            handles.add(new Handle(key, blockOffset, blockSize));
            offset = keyOffset + keyLength + Long.BYTES + Integer.BYTES;
        }
        Encoding.check(offset == size, "trailing block index data");
        return new BlockIndex(handles);
    }

    byte[] encode() {
        int size = 0;
        for (Handle handle : handles) {
            size += Integer.BYTES + handle.firstKey().length() + Long.BYTES + Integer.BYTES;
        }
        var buffer = Encoding.allocate(size);
        for (Handle handle : handles) {
            buffer.putInt(handle.firstKey().length());
            buffer.put(handle.firstKey().toByteArray());
            buffer.putLong(handle.offset());
            buffer.putInt(handle.size());
        }
        return buffer.array();
    }

    int size() {
        return handles.size();
    }

    Handle get(int position) {
        return handles.get(position);
    }

    int find(Slice key) {
        int low = 0;
        int high = handles.size() - 1;
        int result = -1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (key.compareTo(handles.get(mid).firstKey()) >= 0) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return result;
    }
}
