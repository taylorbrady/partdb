package io.partdb.storage;

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

final class Block implements Iterable<StoredEntry> {

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

        long offsetTableBytes = (long) entryCount * Integer.BYTES;
        if (entryCount < 0) {
            throw new StorageException.Corruption("Negative block entry count");
        }
        if (offsetTableStart < 0 || offsetTableStart > trailerOffset) {
            throw new StorageException.Corruption("Invalid block offset table start");
        }
        if (offsetTableStart + offsetTableBytes > trailerOffset) {
            throw new StorageException.Corruption("Block offset table exceeds block bounds");
        }

        return new Block(segment, entryCount, offsetTableStart);
    }

    public Optional<StoredEntry> find(Slice key) {
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

    public StoredEntry entry(int index) {
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
    public Iterator<StoredEntry> iterator() {
        return new BlockIterator();
    }

    private int getEntryOffset(int index) {
        int offset = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offsetTableStart + (index * 4L));
        if (offset < 0 || offset >= offsetTableStart) {
            throw new StorageException.Corruption("Invalid block entry offset: " + offset);
        }
        return offset;
    }

    private Slice keyAt(int index) {
        int offset = getEntryOffset(index);
        if (offset + 13L > offsetTableStart) {
            throw new StorageException.Corruption("Truncated block entry header");
        }
        int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 9);
        if (keyLength < 0) {
            throw new StorageException.Corruption("Negative block key length");
        }
        if (offset + 13L + keyLength > offsetTableStart) {
            throw new StorageException.Corruption("Truncated block key");
        }
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

    private StoredEntry parseEntryAt(int offset) {
        if (offset + 13L > offsetTableStart) {
            throw new StorageException.Corruption("Truncated block entry header");
        }
        byte flags = segment.get(ValueLayout.JAVA_BYTE, offset);
        boolean tombstone = (flags & 0x01) != 0;

        long revision = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 1);

        int keyLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, offset + 9);
        if (keyLength < 0) {
            throw new StorageException.Corruption("Negative block key length");
        }
        if (offset + 13L + keyLength + Integer.BYTES > offsetTableStart) {
            throw new StorageException.Corruption("Truncated block value header");
        }
        Slice key = Slice.wrap(segment.asSlice(offset + 13, keyLength));

        int valueOffset = offset + 13 + keyLength;
        int valueLength = segment.get(ValueLayout.JAVA_INT_UNALIGNED, valueOffset);
        if (valueLength < 0) {
            throw new StorageException.Corruption("Negative block value length");
        }
        if (valueOffset + 4L + valueLength > offsetTableStart) {
            throw new StorageException.Corruption("Truncated block value");
        }

        if (tombstone) {
            if (valueLength != 0) {
                throw new StorageException.Corruption("Tombstone block entry must not carry a value");
            }
            return new StoredEntry.Tombstone(key, revision);
        } else {
            Slice value = Slice.wrap(segment.asSlice(valueOffset + 4, valueLength));
            return new StoredEntry.Value(key, value, revision);
        }
    }

    private final class BlockIterator implements Iterator<StoredEntry> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            return index < entryCount;
        }

        @Override
        public StoredEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return entry(index++);
        }
    }

    public static final class Builder {

        private final List<StoredEntry> entries = new ArrayList<>();
        private final List<Integer> offsets = new ArrayList<>();
        private int currentOffset = 0;

        public Builder add(StoredEntry entry) {
            offsets.add(currentOffset);
            entries.add(entry);
            currentOffset += entry.encodedSizeBytes();
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

            for (StoredEntry entry : entries) {
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

        private void writeEntry(ByteBuffer buffer, StoredEntry entry) {
            switch (entry) {
                case StoredEntry.Tombstone t -> {
                    buffer.put((byte) 0x01);
                    buffer.putLong(t.revision());
                    buffer.putInt(t.key().length());
                    buffer.put(t.key().toByteArray());
                    buffer.putInt(0);
                }
                case StoredEntry.Value value -> {
                    buffer.put((byte) 0x00);
                    buffer.putLong(value.revision());
                    buffer.putInt(value.key().length());
                    buffer.put(value.key().toByteArray());
                    buffer.putInt(value.value().length());
                    buffer.put(value.value().toByteArray());
                }
            }
        }
    }
}
