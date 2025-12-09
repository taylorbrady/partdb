package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.zip.CRC32C;

public final class Block implements Iterable<Entry> {

    private static final int TRAILER_SIZE = 12;

    private final MemorySegment segment;
    private final int entryCount;
    private final int offsetTableStart;

    private Block(MemorySegment segment, int entryCount, int offsetTableStart) {
        this.segment = segment;
        this.entryCount = entryCount;
        this.offsetTableStart = offsetTableStart;
    }

    public static Block from(MemorySegment segment) {
        long size = segment.byteSize();
        if (size < TRAILER_SIZE) {
            throw new SSTableException("Block too small: " + size);
        }

        long trailerOffset = size - TRAILER_SIZE;
        int entryCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, trailerOffset);
        int offsetTableStart = segment.get(ValueLayout.JAVA_INT_UNALIGNED, trailerOffset + 4);
        int storedChecksum = segment.get(ValueLayout.JAVA_INT_UNALIGNED, trailerOffset + 8);

        long checksumDataLength = size - 4;
        byte[] checksumData = segment.asSlice(0, checksumDataLength).toArray(ValueLayout.JAVA_BYTE);
        CRC32C crc = new CRC32C();
        crc.update(checksumData);
        int computedChecksum = (int) crc.getValue();

        if (storedChecksum != computedChecksum) {
            throw new SSTableException("Block checksum mismatch");
        }

        return new Block(segment, entryCount, offsetTableStart);
    }

    public Optional<Entry> find(ByteArray key, Timestamp readTimestamp) {
        int index = binarySearchKey(key);
        if (index < 0) {
            return Optional.empty();
        }

        Entry best = null;
        for (int i = index; i < entryCount; i++) {
            Entry entry = entry(i);
            if (!entry.key().equals(key)) {
                break;
            }
            if (entry.timestamp().compareTo(readTimestamp) <= 0) {
                if (best == null || entry.timestamp().compareTo(best.timestamp()) > 0) {
                    best = entry;
                }
            }
        }

        return Optional.ofNullable(best);
    }

    public int entryCount() {
        return entryCount;
    }

    public Entry entry(int index) {
        if (index < 0 || index >= entryCount) {
            throw new IndexOutOfBoundsException("index: " + index + ", entryCount: " + entryCount);
        }
        return parseEntryAt(getEntryOffset(index));
    }

    public ByteArray firstKey() {
        if (entryCount == 0) {
            throw new IllegalStateException("Block is empty");
        }
        return keyAt(0);
    }

    public ByteArray lastKey() {
        if (entryCount == 0) {
            throw new IllegalStateException("Block is empty");
        }
        return keyAt(entryCount - 1);
    }

    @Override
    public Iterator<Entry> iterator() {
        return new BlockIterator();
    }

    private int getEntryOffset(int index) {
        return segment.get(ValueLayout.JAVA_INT_UNALIGNED, offsetTableStart + (index * 4L));
    }

    private ByteArray keyAt(int index) {
        int offset = getEntryOffset(index);
        int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 9);
        byte[] keyBytes = segment.asSlice(offset + 13, keyLength).toArray(ValueLayout.JAVA_BYTE);
        return ByteArray.copyOf(keyBytes);
    }

    private int binarySearchKey(ByteArray key) {
        int low = 0;
        int high = entryCount - 1;
        int result = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            ByteArray midKey = keyAt(mid);
            int cmp = midKey.compareTo(key);

            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                result = mid;
                high = mid - 1;
            }
        }

        return result;
    }

    private Entry parseEntryAt(int offset) {
        byte flags = segment.get(ValueLayout.JAVA_BYTE, offset);
        boolean tombstone = (flags & 0x01) != 0;

        long timestampValue = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 1);
        Timestamp timestamp = new Timestamp(timestampValue);

        int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 9);
        byte[] keyBytes = segment.asSlice(offset + 13, keyLength).toArray(ValueLayout.JAVA_BYTE);
        ByteArray key = ByteArray.copyOf(keyBytes);

        int valueOffset = offset + 13 + keyLength;
        int valueLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, valueOffset);

        if (tombstone) {
            return new Entry.Tombstone(key, timestamp);
        } else {
            byte[] valueBytes = segment.asSlice(valueOffset + 4, valueLength).toArray(ValueLayout.JAVA_BYTE);
            return new Entry.Put(key, timestamp, ByteArray.copyOf(valueBytes));
        }
    }

    private final class BlockIterator implements Iterator<Entry> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < entryCount;
        }

        @Override
        public Entry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return entry(index++);
        }
    }

    public static final class Builder {

        private final List<Entry> entries = new ArrayList<>();
        private final List<Integer> offsets = new ArrayList<>();
        private int currentOffset = 0;

        public Builder add(Entry entry) {
            offsets.add(currentOffset);
            entries.add(entry);
            currentOffset += entrySize(entry);
            return this;
        }

        public int entryCount() {
            return entries.size();
        }

        public int estimatedSize() {
            return currentOffset + (entries.size() * 4) + TRAILER_SIZE;
        }

        public boolean isEmpty() {
            return entries.isEmpty();
        }

        public ByteArray firstKey() {
            if (entries.isEmpty()) {
                throw new IllegalStateException("Block is empty");
            }
            return entries.getFirst().key();
        }

        public ByteArray lastKey() {
            if (entries.isEmpty()) {
                throw new IllegalStateException("Block is empty");
            }
            return entries.getLast().key();
        }

        public byte[] build() {
            if (entries.isEmpty()) {
                throw new IllegalStateException("Cannot build empty block");
            }

            int offsetTableStart = currentOffset;
            int totalSize = currentOffset + (entries.size() * 4) + TRAILER_SIZE;

            ByteBuffer buffer = ByteBuffer.allocate(totalSize).order(ByteOrder.nativeOrder());

            for (Entry entry : entries) {
                writeEntry(buffer, entry);
            }

            for (int offset : offsets) {
                buffer.putInt(offset);
            }

            buffer.putInt(entries.size());
            buffer.putInt(offsetTableStart);

            CRC32C crc = new CRC32C();
            crc.update(buffer.array(), 0, buffer.position());
            buffer.putInt((int) crc.getValue());

            return buffer.array();
        }

        public Builder reset() {
            entries.clear();
            offsets.clear();
            currentOffset = 0;
            return this;
        }

        private void writeEntry(ByteBuffer buffer, Entry entry) {
            byte[] keyBytes = entry.key().toByteArray();

            switch (entry) {
                case Entry.Tombstone t -> {
                    buffer.put((byte) 0x01);
                    buffer.putLong(t.timestamp().value());
                    buffer.putInt(keyBytes.length);
                    buffer.put(keyBytes);
                    buffer.putInt(0);
                }
                case Entry.Put p -> {
                    buffer.put((byte) 0x00);
                    buffer.putLong(p.timestamp().value());
                    buffer.putInt(keyBytes.length);
                    buffer.put(keyBytes);
                    byte[] valueBytes = p.value().toByteArray();
                    buffer.putInt(valueBytes.length);
                    buffer.put(valueBytes);
                }
            }
        }

        private int entrySize(Entry entry) {
            return switch (entry) {
                case Entry.Put p -> 1 + 8 + 4 + entry.key().length() + 4 + p.value().length();
                case Entry.Tombstone _ -> 1 + 8 + 4 + entry.key().length() + 4;
            };
        }
    }
}
