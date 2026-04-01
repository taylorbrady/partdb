package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public final class BenchmarkKeys {

    private static final byte[] KEY_PREFIX = "key".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MISSING_PREFIX = "missing".getBytes(StandardCharsets.US_ASCII);
    private static final int KEY_WIDTH = 16;

    private BenchmarkKeys() {
    }

    public static Bytes storageKey(long value) {
        return Bytes.copyOf(encodePaddedDecimal(KEY_PREFIX, value, KEY_WIDTH));
    }

    public static Bytes missingKey(long value) {
        return Bytes.copyOf(encodePaddedDecimal(MISSING_PREFIX, value, KEY_WIDTH));
    }

    public static Bytes[] storageKeys(int count) {
        return storageKeys(0, count);
    }

    public static Bytes[] storageKeys(long startValue, int count) {
        Bytes[] keys = new Bytes[count];
        for (int i = 0; i < count; i++) {
            keys[i] = storageKey(startValue + i);
        }
        return keys;
    }

    public static Bytes[] missingKeys(int count) {
        Bytes[] keys = new Bytes[count];
        for (int i = 0; i < count; i++) {
            keys[i] = missingKey(i);
        }
        return keys;
    }

    public static Bytes[] shuffledStorageKeys(int count, long seed) {
        Bytes[] keys = storageKeys(count);
        Random random = new Random(seed);
        for (int i = keys.length - 1; i > 0; i--) {
            int swapIndex = random.nextInt(i + 1);
            Bytes tmp = keys[i];
            keys[i] = keys[swapIndex];
            keys[swapIndex] = tmp;
        }
        return keys;
    }

    private static byte[] encodePaddedDecimal(byte[] prefix, long value, int width) {
        if (value < 0) {
            throw new IllegalArgumentException("value must be non-negative");
        }

        byte[] bytes = new byte[prefix.length + width];
        System.arraycopy(prefix, 0, bytes, 0, prefix.length);

        long remaining = value;
        for (int i = bytes.length - 1; i >= prefix.length; i--) {
            bytes[i] = (byte) ('0' + (remaining % 10));
            remaining /= 10;
        }

        if (remaining != 0) {
            throw new IllegalArgumentException("value " + value + " exceeds width " + width);
        }

        return bytes;
    }
}
