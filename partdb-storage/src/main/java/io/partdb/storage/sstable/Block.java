package io.partdb.storage.sstable;

import io.partdb.common.Slice;
import io.partdb.storage.Mutation;
import io.partdb.storage.StorageException;

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

public final class Block implements Iterable<Mutation> {

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
            throw new StorageException.Corruption("Block too small: " + size);
        }

        long trailerOffset = size - TRAILER_SIZE;
        int entryCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, trailerOffset);
        int offsetTableStart = segment.get(ValueLayout.JAVA_INT_UNALIGNED, trailerOffset + 4);
        int storedChecksum = segment.get(ValueLayout.JAVA_INT_UNALIGNED, trailerOffset + 8);

        long checksumDataLength = size - 4;
        CRC32C crc = new CRC32C();
        crc.update(segment.asByteBuffer().limit((int) checksumDataLength));
        int computedChecksum = (int) crc.getValue();

        if (storedChecksum != computedChecksum) {
            throw new StorageException.Corruption("Block checksum mismatch");
        }

        return new Block(segment, entryCount, offsetTableStart);
    }

    public Optional<Mutation> find(Slice key) {
        int index = binarySearchKey(key);
        if (index < 0) {
            return Optional.empty();
        }
        return Optional.of(entry(index));
    }

    public int entryCount() {
        return entryCount;
    }

    public long sizeInBytes() {
        return segment.byteSize();
    }

    public Mutation entry(int index) {
        if (index < 0 || index >= entryCount) {
            throw new IndexOutOfBoundsException("index: " + index + ", entryCount: " + entryCount);
        }
        return parseEntryAt(getEntryOffset(index));
    }

    public Slice firstKey() {
        if (entryCount == 0) {
            throw new IllegalStateException("Block is empty");
        }
        return keyAt(0);
    }

    public Slice lastKey() {
        if (entryCount == 0) {
            throw new IllegalStateException("Block is empty");
        }
        return keyAt(entryCount - 1);
    }

    @Override
    public Iterator<Mutation> iterator() {
        return new BlockIterator();
    }

    private int getEntryOffset(int index) {
        return segment.get(ValueLayout.JAVA_INT_UNALIGNED, offsetTableStart + (index * 4L));
    }

    private Slice keyAt(int index) {
        int offset = getEntryOffset(index);
        int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 9);
        return Slice.wrap(segment.asSlice(offset + 13, keyLength));
    }

    private int binarySearchKey(Slice key) {
        int low = 0;
        int high = entryCount - 1;
        int result = -1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Slice midKey = keyAt(mid);
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

    private Mutation parseEntryAt(int offset) {
        byte flags = segment.get(ValueLayout.JAVA_BYTE, offset);
        boolean tombstone = (flags & 0x01) != 0;

        long revision = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 1);

        int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 9);
        Slice key = Slice.wrap(segment.asSlice(offset + 13, keyLength));

        int valueOffset = offset + 13 + keyLength;
        int valueLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, valueOffset);

        if (tombstone) {
            return new Mutation.Tombstone(key, revision);
        } else {
            Slice value = Slice.wrap(segment.asSlice(valueOffset + 4, valueLength));
            return new Mutation.Put(key, value, revision);
        }
    }

    private final class BlockIterator implements Iterator<Mutation> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < entryCount;
        }

        @Override
        public Mutation next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return entry(index++);
        }
    }

    public static final class Builder {

        private final List<Mutation> entries = new ArrayList<>();
        private final List<Integer> offsets = new ArrayList<>();
        private int currentOffset = 0;

        public Builder add(Mutation mutation) {
            offsets.add(currentOffset);
            entries.add(mutation);
            currentOffset += entrySize(mutation);
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

        public Slice firstKey() {
            if (entries.isEmpty()) {
                throw new IllegalStateException("Block is empty");
            }
            return entries.getFirst().key();
        }

        public Slice lastKey() {
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

            for (Mutation mutation : entries) {
                writeEntry(buffer, mutation);
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

        private void writeEntry(ByteBuffer buffer, Mutation mutation) {
            switch (mutation) {
                case Mutation.Tombstone t -> {
                    buffer.put((byte) 0x01);
                    buffer.putLong(t.revision());
                    buffer.putInt(t.key().length());
                    buffer.put(t.key().toByteArray());
                    buffer.putInt(0);
                }
                case Mutation.Put p -> {
                    buffer.put((byte) 0x00);
                    buffer.putLong(p.revision());
                    buffer.putInt(p.key().length());
                    buffer.put(p.key().toByteArray());
                    buffer.putInt(p.value().length());
                    buffer.put(p.value().toByteArray());
                }
            }
        }

        private int entrySize(Mutation mutation) {
            return switch (mutation) {
                case Mutation.Put p -> 1 + 8 + 4 + mutation.key().length() + 4 + p.value().length();
                case Mutation.Tombstone _ -> 1 + 8 + 4 + mutation.key().length() + 4;
            };
        }
    }
}
