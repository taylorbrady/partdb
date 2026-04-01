package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

final class DataBlockReader {

    private static final int TRAILER_SIZE = Integer.BYTES * 2;
    private static final byte TOMBSTONE_FLAG = 0x01;

    private final MemorySegment segment;
    private final int entryCount;
    private final int[] restartOffsets;
    private final long dataLimit;
    private final Slice firstKey;
    private final Slice lastKey;

    private DataBlockReader(
        MemorySegment segment,
        int entryCount,
        int[] restartOffsets,
        long dataLimit,
        Slice firstKey,
        Slice lastKey
    ) {
        this.segment = segment.isReadOnly() ? segment : segment.asReadOnly();
        this.entryCount = entryCount;
        this.restartOffsets = restartOffsets;
        this.dataLimit = dataLimit;
        this.firstKey = firstKey;
        this.lastKey = lastKey;
    }

    static DataBlockReader from(MemorySegment segment) {
        long size = segment.byteSize();
        if (size < TRAILER_SIZE) {
            throw new StorageException.Corruption("DataBlock too small: " + size);
        }

        int entryCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, size - Integer.BYTES);
        int restartCount = segment.get(ValueLayout.JAVA_INT_UNALIGNED, size - TRAILER_SIZE);
        if (entryCount <= 0) {
            throw new StorageException.Corruption("DataBlock entry count must be positive");
        }
        if (restartCount <= 0) {
            throw new StorageException.Corruption("DataBlock restart count must be positive");
        }
        if (restartCount > entryCount) {
            throw new StorageException.Corruption("DataBlock restart count exceeds entry count");
        }

        long restartTableOffset = size - TRAILER_SIZE - (restartCount * (long) Integer.BYTES);
        if (restartTableOffset <= 0) {
            throw new StorageException.Corruption("DataBlock restart table overlaps data region");
        }

        int[] restartOffsets = new int[restartCount];
        int previousOffset = -1;
        for (int i = 0; i < restartCount; i++) {
            int restartOffset = segment.get(
                ValueLayout.JAVA_INT_UNALIGNED,
                restartTableOffset + (i * (long) Integer.BYTES)
            );
            if (restartOffset < 0 || restartOffset >= restartTableOffset) {
                throw new StorageException.Corruption("Invalid data block restart offset: " + restartOffset);
            }
            if (i == 0 && restartOffset != 0) {
                throw new StorageException.Corruption("First data block restart offset must be 0");
            }
            if (restartOffset <= previousOffset) {
                throw new StorageException.Corruption("Data block restart offsets must be strictly increasing");
            }
            restartOffsets[i] = restartOffset;
            previousOffset = restartOffset;
        }

        DataBlockReader provisional = new DataBlockReader(
            segment,
            entryCount,
            restartOffsets,
            restartTableOffset,
            Slice.empty(),
            Slice.empty()
        );
        Slice firstKey = provisional.restartKeyAt(0);
        Slice lastKey = provisional.computeLastKey();
        return new DataBlockReader(segment, entryCount, restartOffsets, restartTableOffset, firstKey, lastKey);
    }

    Optional<StoredEntry> find(Slice key) {
        DataBlockCursor cursor = cursorAtOrAfter(key);
        if (!cursor.hasNext()) {
            return Optional.empty();
        }
        StoredEntry candidate = cursor.next();
        return candidate.key().equals(key) ? Optional.of(candidate) : Optional.empty();
    }

    DataBlockCursor cursor() {
        return new DataBlockCursor(this, 0, dataLimit);
    }

    DataBlockCursor cursorAtOrAfter(Slice key) {
        int restartIndex = restartIndexFor(key);
        DataBlockCursor cursor = new DataBlockCursor(this, restartOffsets[restartIndex], dataLimit);
        cursor.advanceToAtOrAfter(key);
        return cursor;
    }

    Slice firstKey() {
        return firstKey;
    }

    Slice lastKey() {
        return lastKey;
    }

    int entryCount() {
        return entryCount;
    }

    long sizeInBytes() {
        return segment.byteSize();
    }

    long dataLimit() {
        return dataLimit;
    }

    DecodedEntry decodeEntry(long offset, byte[] previousKeyBytes, long limit) {
        if (offset < 0 || offset >= limit) {
            throw new StorageException.Corruption("Invalid data block entry offset: " + offset);
        }

        VarInt sharedPrefix = readUnsignedVarInt(offset, limit, "shared prefix length");
        VarInt unsharedKeyLength = readUnsignedVarInt(sharedPrefix.nextOffset(), limit, "unshared key length");
        VarInt valueLength = readUnsignedVarInt(unsharedKeyLength.nextOffset(), limit, "value length");

        long flagsOffset = valueLength.nextOffset();
        if (flagsOffset + 1 + Long.BYTES > limit) {
            throw new StorageException.Corruption("Truncated data block entry header");
        }

        int sharedLength = sharedPrefix.value();
        byte[] previous = previousKeyBytes == null ? EMPTY_BYTES : previousKeyBytes;
        if (previousKeyBytes == null && sharedLength != 0) {
            throw new StorageException.Corruption("Restart entry must not use a shared prefix");
        }
        if (sharedLength > previous.length) {
            throw new StorageException.Corruption("Shared prefix length exceeds previous key length");
        }

        byte flags = segment.get(ValueLayout.JAVA_BYTE, flagsOffset);
        long revision = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, flagsOffset + 1);

        int suffixLength = unsharedKeyLength.value();
        int encodedValueLength = valueLength.value();
        long keySuffixOffset = flagsOffset + 1 + Long.BYTES;
        long valueOffset = keySuffixOffset + suffixLength;

        if (keySuffixOffset + suffixLength > limit) {
            throw new StorageException.Corruption("Truncated data block key suffix");
        }
        if (valueOffset + encodedValueLength > limit) {
            throw new StorageException.Corruption("Truncated data block value");
        }

        byte[] keyBytes = new byte[sharedLength + suffixLength];
        System.arraycopy(previous, 0, keyBytes, 0, sharedLength);
        for (int i = 0; i < suffixLength; i++) {
            keyBytes[sharedLength + i] = segment.get(ValueLayout.JAVA_BYTE, keySuffixOffset + i);
        }
        Slice key = Slice.wrap(MemorySegment.ofArray(keyBytes));

        long nextOffset = valueOffset + encodedValueLength;
        if ((flags & TOMBSTONE_FLAG) != 0) {
            if (encodedValueLength != 0) {
                throw new StorageException.Corruption("Tombstone data block entry must not carry a value");
            }
            return new DecodedEntry(new StoredEntry.Tombstone(key, revision), keyBytes, nextOffset);
        }

        Slice value = Slice.wrap(segment.asSlice(valueOffset, encodedValueLength));
        return new DecodedEntry(new StoredEntry.Value(key, value, revision), keyBytes, nextOffset);
    }

    private Slice restartKeyAt(int restartIndex) {
        return decodeEntry(restartOffsets[restartIndex], null, dataLimit).entry().key();
    }

    private Slice computeLastKey() {
        DataBlockCursor cursor = new DataBlockCursor(this, restartOffsets[restartOffsets.length - 1], dataLimit);
        Slice last = null;
        while (cursor.hasNext()) {
            last = cursor.next().key();
        }
        if (last == null) {
            throw new StorageException.Corruption("Data block restart region is empty");
        }
        return last;
    }

    private int restartIndexFor(Slice key) {
        int low = 0;
        int high = restartOffsets.length - 1;
        int result = 0;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int comparison = key.compareTo(restartKeyAt(mid));
            if (comparison >= 0) {
                result = mid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }

        return result;
    }

    private VarInt readUnsignedVarInt(long offset, long limit, String label) {
        long current = offset;
        int value = 0;
        int shift = 0;

        while (current < limit) {
            int next = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, current));
            current++;

            value |= (next & 0x7f) << shift;
            if ((next & 0x80) == 0) {
                return new VarInt(value, current);
            }

            shift += 7;
            if (shift >= Integer.SIZE) {
                throw new StorageException.Corruption("Data block " + label + " varint is too long");
            }
        }

        throw new StorageException.Corruption("Truncated data block " + label + " varint");
    }

    record DecodedEntry(StoredEntry entry, byte[] keyBytes, long nextOffset) {}

    private record VarInt(int value, long nextOffset) {}

    private static final byte[] EMPTY_BYTES = new byte[0];
}
