package io.partdb.storage;

import java.io.ByteArrayOutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

final class DataBlockWriter {

    private static final int TRAILER_SIZE = Integer.BYTES * 2;
    private static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    private final int restartInterval;
    private final ByteArrayOutputStream data;
    private final List<Integer> restartOffsets;

    private Slice firstKey;
    private Slice lastKey;
    private InternalKey lastInternalKey;
    private byte[] lastKeyBytes;
    private int entriesSinceRestart;
    private int entryCount;

    DataBlockWriter(int restartInterval) {
        if (restartInterval <= 0) {
            throw new IllegalArgumentException("restartInterval must be positive");
        }
        this.restartInterval = restartInterval;
        this.data = new ByteArrayOutputStream();
        this.restartOffsets = new ArrayList<>();
    }

    void append(InternalEntry entry) {
        if (lastInternalKey != null && entry.key().compareTo(lastInternalKey) <= 0) {
            throw new IllegalArgumentException("Internal key ordering must be strictly increasing");
        }

        if (lastKey != null && entry.userKey().compareTo(lastKey) < 0) {
            throw new IllegalArgumentException(
                "Entries must be added in ascending user-key order: last=%s, current=%s"
                    .formatted(lastKey, entry.userKey())
            );
        }

        byte[] keyBytes = entry.userKey().toByteArray();
        byte[] valueBytes = switch (entry) {
            case InternalEntry.Tombstone _ -> null;
            case InternalEntry.Value value -> value.value().toByteArray();
        };

        int sharedPrefixLength = 0;
        if (entryCount == 0 || entriesSinceRestart >= restartInterval) {
            restartOffsets.add(data.size());
            entriesSinceRestart = 0;
        } else {
            sharedPrefixLength = sharedPrefixLength(lastKeyBytes, keyBytes);
        }

        int unsharedKeyLength = keyBytes.length - sharedPrefixLength;
        writeUnsignedVarInt(data, sharedPrefixLength);
        writeUnsignedVarInt(data, unsharedKeyLength);
        writeUnsignedVarInt(data, valueBytes == null ? 0 : valueBytes.length);
        data.write(entry instanceof InternalEntry.Tombstone ? 0x01 : 0x00);
        writeLong(data, entry.revision());
        data.write(keyBytes, sharedPrefixLength, unsharedKeyLength);
        if (valueBytes != null) {
            data.write(valueBytes, 0, valueBytes.length);
        }

        if (firstKey == null) {
            firstKey = entry.userKey();
        }
        lastKey = entry.userKey();
        lastInternalKey = entry.key();
        lastKeyBytes = keyBytes;
        entryCount++;
        entriesSinceRestart++;
    }

    void append(StoredEntry entry) {
        append(InternalEntry.from(entry));
    }

    boolean isEmpty() {
        return entryCount == 0;
    }

    int estimatedSizeBytes() {
        return data.size() + (restartOffsets.size() * Integer.BYTES) + TRAILER_SIZE;
    }

    Slice firstKey() {
        if (firstKey == null) {
            throw new IllegalStateException("Data block is empty");
        }
        return firstKey;
    }

    Slice lastKey() {
        if (lastKey == null) {
            throw new IllegalStateException("Data block is empty");
        }
        return lastKey;
    }

    byte[] finish() {
        if (isEmpty()) {
            throw new IllegalStateException("Cannot finish empty data block");
        }

        ByteArrayOutputStream output = new ByteArrayOutputStream(estimatedSizeBytes());
        output.writeBytes(data.toByteArray());
        for (int restartOffset : restartOffsets) {
            writeInt(output, restartOffset);
        }
        writeInt(output, restartOffsets.size());
        writeInt(output, entryCount);
        return output.toByteArray();
    }

    void reset() {
        data.reset();
        restartOffsets.clear();
        firstKey = null;
        lastKey = null;
        lastInternalKey = null;
        lastKeyBytes = null;
        entriesSinceRestart = 0;
        entryCount = 0;
    }

    private static int sharedPrefixLength(byte[] left, byte[] right) {
        int limit = Math.min(left.length, right.length);
        int shared = 0;
        while (shared < limit && left[shared] == right[shared]) {
            shared++;
        }
        return shared;
    }

    private static void writeUnsignedVarInt(ByteArrayOutputStream output, int value) {
        int remaining = value;
        while ((remaining & ~0x7f) != 0) {
            output.write((remaining & 0x7f) | 0x80);
            remaining >>>= 7;
        }
        output.write(remaining);
    }

    private static void writeInt(ByteArrayOutputStream output, int value) {
        if (LITTLE_ENDIAN) {
            output.write(value & 0xff);
            output.write((value >>> 8) & 0xff);
            output.write((value >>> 16) & 0xff);
            output.write((value >>> 24) & 0xff);
        } else {
            output.write((value >>> 24) & 0xff);
            output.write((value >>> 16) & 0xff);
            output.write((value >>> 8) & 0xff);
            output.write(value & 0xff);
        }
    }

    private static void writeLong(ByteArrayOutputStream output, long value) {
        if (LITTLE_ENDIAN) {
            for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
                output.write((int) ((value >>> shift) & 0xff));
            }
        } else {
            for (int shift = Long.SIZE - Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
                output.write((int) ((value >>> shift) & 0xff));
            }
        }
    }
}
