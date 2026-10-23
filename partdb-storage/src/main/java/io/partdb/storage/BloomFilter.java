package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

final class BloomFilter {

    private static final int LINE_BITS = 512;
    private static final int LINE_LONGS = LINE_BITS / Long.SIZE;
    private static final int HEADER = 2 * Integer.BYTES;
    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    private final long[] bits;
    private final int lines;
    private final int hashes;

    private BloomFilter(long[] bits, int lines, int hashes) {
        this.bits = bits;
        this.lines = lines;
        this.hashes = hashes;
    }

    static BloomFilter build(List<Slice> keys, double falsePositiveRate) {
        int expected = Math.max(1, keys.size());
        int bitsNeeded = (int) Math.ceil(-(expected * Math.log(falsePositiveRate)) / (Math.log(2) * Math.log(2)));
        int lines = Math.max(1, (bitsNeeded + LINE_BITS - 1) / LINE_BITS);
        int hashes = Math.clamp(Math.round((double) lines * LINE_BITS / expected * Math.log(2)), 1, 30);
        BloomFilter filter = new BloomFilter(new long[lines * LINE_LONGS], lines, hashes);
        for (Slice key : keys) {
            filter.add(key);
        }
        return filter;
    }

    static BloomFilter decode(MemorySegment segment) {
        long size = segment.byteSize();
        Encoding.check(size >= HEADER, "bloom filter too small: " + size);
        int hashes = segment.get(Encoding.INT, 0);
        int lines = segment.get(Encoding.INT, Integer.BYTES);
        Encoding.check(hashes > 0 && lines > 0, "invalid bloom filter header");
        Encoding.check(size == HEADER + (long) lines * LINE_LONGS * Long.BYTES, "bloom filter size mismatch");
        long[] bits = new long[lines * LINE_LONGS];
        for (int i = 0; i < bits.length; i++) {
            bits[i] = segment.get(Encoding.LONG, HEADER + (long) i * Long.BYTES);
        }
        return new BloomFilter(bits, lines, hashes);
    }

    byte[] encode() {
        var buffer = Encoding.allocate(HEADER + bits.length * Long.BYTES);
        buffer.putInt(hashes);
        buffer.putInt(lines);
        for (long word : bits) {
            buffer.putLong(word);
        }
        return buffer.array();
    }

    boolean mightContain(Slice key) {
        int h1 = murmur3(key, 0);
        int h2 = murmur3(key, 1);
        int line = line(h1, h2);
        for (int i = 0; i < hashes; i++) {
            int bit = Integer.remainderUnsigned(h1 + (i + 1) * h2, LINE_BITS);
            if ((bits[line + (bit >>> 6)] & (1L << bit)) == 0) {
                return false;
            }
        }
        return true;
    }

    private void add(Slice key) {
        int h1 = murmur3(key, 0);
        int h2 = murmur3(key, 1);
        int line = line(h1, h2);
        for (int i = 0; i < hashes; i++) {
            int bit = Integer.remainderUnsigned(h1 + (i + 1) * h2, LINE_BITS);
            bits[line + (bit >>> 6)] |= 1L << bit;
        }
    }

    private int line(int h1, int h2) {
        int mixed = (Integer.rotateLeft(h1, 16) ^ h2) * 0x9e3779b9;
        return Integer.remainderUnsigned(mixed ^ (mixed >>> 15), lines) * LINE_LONGS;
    }

    private static int murmur3(Slice key, int seed) {
        MemorySegment segment = key.segment();
        int length = key.length();
        int hash = seed;
        int rounded = length & ~3;
        for (int i = 0; i < rounded; i += 4) {
            hash ^= mix(segment.get(Encoding.INT, i));
            hash = Integer.rotateLeft(hash, 13) * 5 + 0xe6546b64;
        }
        int tail = 0;
        for (int i = length - 1; i >= rounded; i--) {
            tail = (tail << 8) | (segment.get(ValueLayout.JAVA_BYTE, i) & 0xff);
        }
        if ((length & 3) != 0) {
            hash ^= mix(tail);
        }
        hash ^= length;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;
        return hash;
    }

    private static int mix(int k) {
        return Integer.rotateLeft(k * C1, 15) * C2;
    }
}
