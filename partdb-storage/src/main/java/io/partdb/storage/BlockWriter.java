package io.partdb.storage;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

final class BlockWriter {

    static final byte TOMBSTONE = 0x01;

    private final int restartInterval;
    private final ByteArrayOutputStream data = new ByteArrayOutputStream();
    private final List<Integer> restarts = new ArrayList<>();

    private Slice firstKey;
    private Slice lastKey;
    private byte[] lastKeyBytes;
    private int sinceRestart;
    private int count;

    BlockWriter(int restartInterval) {
        if (restartInterval <= 0) {
            throw new IllegalArgumentException("restartInterval must be positive");
        }
        this.restartInterval = restartInterval;
    }

    void add(InternalEntry entry) {
        if (lastKey != null && entry.userKey().compareTo(lastKey) <= 0) {
            throw new IllegalArgumentException("Block keys must be strictly increasing");
        }

        byte[] key = entry.userKey().toByteArray();
        byte[] value = switch (entry) {
            case InternalEntry.Value v -> v.value().toByteArray();
            case InternalEntry.Tombstone _ -> new byte[0];
        };

        int shared = 0;
        if (count == 0 || sinceRestart >= restartInterval) {
            restarts.add(data.size());
            sinceRestart = 0;
        } else {
            shared = sharedPrefix(lastKeyBytes, key);
        }

        writeVarInt(shared);
        writeVarInt(key.length - shared);
        writeVarInt(value.length);
        data.write(entry instanceof InternalEntry.Tombstone ? TOMBSTONE : 0);
        writeLong(entry.revision());
        data.write(key, shared, key.length - shared);
        data.write(value, 0, value.length);

        if (firstKey == null) {
            firstKey = entry.userKey();
        }
        lastKey = entry.userKey();
        lastKeyBytes = key;
        sinceRestart++;
        count++;
    }

    boolean isEmpty() {
        return count == 0;
    }

    int estimatedSize() {
        return data.size() + (restarts.size() + 2) * Integer.BYTES;
    }

    Slice firstKey() {
        return firstKey;
    }

    byte[] finish() {
        if (count == 0) {
            throw new IllegalStateException("Cannot finish an empty block");
        }
        var trailer = Encoding.allocate((restarts.size() + 2) * Integer.BYTES);
        for (int restart : restarts) {
            trailer.putInt(restart);
        }
        trailer.putInt(restarts.size());
        trailer.putInt(count);
        data.writeBytes(trailer.array());
        byte[] block = data.toByteArray();
        reset();
        return block;
    }

    private void reset() {
        data.reset();
        restarts.clear();
        firstKey = null;
        lastKey = null;
        lastKeyBytes = null;
        sinceRestart = 0;
        count = 0;
    }

    private void writeVarInt(int value) {
        int remaining = value;
        while ((remaining & ~0x7f) != 0) {
            data.write((remaining & 0x7f) | 0x80);
            remaining >>>= 7;
        }
        data.write(remaining);
    }

    private void writeLong(long value) {
        for (int shift = 0; shift < Long.SIZE; shift += Byte.SIZE) {
            data.write((int) (value >>> shift));
        }
    }

    private static int sharedPrefix(byte[] left, byte[] right) {
        int limit = Math.min(left.length, right.length);
        int shared = 0;
        while (shared < limit && left[shared] == right[shared]) {
            shared++;
        }
        return shared;
    }
}
