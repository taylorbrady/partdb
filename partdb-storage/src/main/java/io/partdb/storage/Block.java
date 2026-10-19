package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;

final class Block {

    private static final byte[] NO_KEY = new byte[0];

    private final MemorySegment segment;
    private final int[] restarts;
    private final long limit;

    private Block(MemorySegment segment, int[] restarts, long limit) {
        this.segment = segment;
        this.restarts = restarts;
        this.limit = limit;
    }

    static Block from(MemorySegment segment) {
        long size = segment.byteSize();
        Encoding.check(size >= 2L * Integer.BYTES, "Block too small: " + size);

        int count = segment.get(Encoding.INT, size - Integer.BYTES);
        int restartCount = segment.get(Encoding.INT, size - 2L * Integer.BYTES);
        Encoding.check(count > 0, "Block entry count must be positive");
        Encoding.check(restartCount > 0 && restartCount <= count, "Invalid block restart count: " + restartCount);

        long limit = size - (2L + restartCount) * Integer.BYTES;
        Encoding.check(limit > 0, "Block restart table overlaps entries");

        int[] restarts = new int[restartCount];
        for (int i = 0; i < restartCount; i++) {
            restarts[i] = segment.get(Encoding.INT, limit + (long) i * Integer.BYTES);
            Encoding.check(restarts[i] >= 0 && restarts[i] < limit, "Invalid block restart offset: " + restarts[i]);
            Encoding.check(i == 0 ? restarts[i] == 0 : restarts[i] > restarts[i - 1], "Block restart offsets out of order");
        }
        return new Block(segment, restarts, limit);
    }

    long sizeBytes() {
        return segment.byteSize();
    }

    Optional<InternalEntry> get(Slice key) {
        Cursor cursor = seek(key);
        return cursor.hasNext() && cursor.peek().userKey().equals(key) ? Optional.of(cursor.peek()) : Optional.empty();
    }

    Iterator<InternalEntry> iterator() {
        return new Cursor(0);
    }

    Cursor seek(Slice key) {
        int low = 0;
        int high = restarts.length - 1;
        int restart = 0;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            if (new Cursor(restarts[mid]).peek().userKey().compareTo(key) < 0) {
                restart = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        Cursor cursor = new Cursor(restarts[restart]);
        while (cursor.hasNext() && cursor.peek().userKey().compareTo(key) < 0) {
            cursor.next();
        }
        return cursor;
    }

    final class Cursor implements Iterator<InternalEntry> {
        private long offset;
        private byte[] previousKey = NO_KEY;
        private InternalEntry next;

        private Cursor(long offset) {
            this.offset = offset;
            advance();
        }

        InternalEntry peek() {
            return next;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public InternalEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            InternalEntry current = next;
            advance();
            return current;
        }

        private void advance() {
            if (offset >= limit) {
                next = null;
                return;
            }
            int shared = readVarInt();
            int unshared = readVarInt();
            int valueLength = readVarInt();
            Encoding.check(offset + 1 + Long.BYTES + unshared + (long) valueLength <= limit, "Truncated block entry");
            Encoding.check(shared <= previousKey.length, "Block shared prefix exceeds previous key");

            byte flags = segment.get(ValueLayout.JAVA_BYTE, offset);
            long revision = segment.get(Encoding.LONG, offset + 1);
            offset += 1 + Long.BYTES;

            byte[] key = new byte[shared + unshared];
            System.arraycopy(previousKey, 0, key, 0, shared);
            MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, offset, key, shared, unshared);
            offset += unshared;
            previousKey = key;

            InternalKey internalKey = new InternalKey(Slice.wrap(key), revision);
            if ((flags & BlockWriter.TOMBSTONE) != 0) {
                Encoding.check(valueLength == 0, "Block tombstone carries a value");
                next = new InternalEntry.Tombstone(internalKey);
            } else {
                next = new InternalEntry.Value(internalKey, Slice.wrap(segment.asSlice(offset, valueLength)));
            }
            offset += valueLength;
        }

        private int readVarInt() {
            int value = 0;
            for (int shift = 0; shift < Integer.SIZE; shift += 7) {
                Encoding.check(offset < limit, "Truncated block varint");
                int b = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, offset++));
                value |= (b & 0x7f) << shift;
                if ((b & 0x80) == 0) {
                    Encoding.check(value >= 0, "Negative block varint");
                    return value;
                }
            }
            throw new StoreException.Corruption("Block varint too long");
        }
    }
}
