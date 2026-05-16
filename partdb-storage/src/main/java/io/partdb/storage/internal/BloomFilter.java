package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

final class BloomFilter {

    private static final int CACHE_LINE_BITS = 512;
    private static final int CACHE_LINE_LONGS = 8;
    private static final int MURMUR3_C1 = 0xcc9e2d51;
    private static final int MURMUR3_C2 = 0x1b873593;

    private final long[] bits;
    private final int numBlocks;
    private final int numHashFunctions;

    private BloomFilter(long[] bits, int numBlocks, int numHashFunctions) {
        this.bits = bits;
        this.numBlocks = numBlocks;
        this.numHashFunctions = numHashFunctions;
    }

    static BloomFilter build(List<Slice> keys, double falsePositiveRate) {
        if (keys.isEmpty()) {
            return create(1, falsePositiveRate);
        }
        BloomFilter filter = create(keys.size(), falsePositiveRate);
        for (Slice key : keys) {
            filter.add(key);
        }
        return filter;
    }

    static BloomFilter from(MemorySegment segment) {
        long size = segment.byteSize();
        if (size < 8) {
            throw new StorageException.Corruption("BloomFilter too small: " + size);
        }

        int numHashFunctions = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int numBlocks = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 4);
        if (numHashFunctions <= 0) {
            throw new StorageException.Corruption("BloomFilter numHashFunctions must be positive");
        }
        if (numBlocks <= 0) {
            throw new StorageException.Corruption("BloomFilter numBlocks must be positive");
        }

        int numLongs = numBlocks * CACHE_LINE_LONGS;
        long expectedSize = 8L + numLongs * Long.BYTES;
        if (size != expectedSize) {
            throw new StorageException.Corruption(
                "BloomFilter size mismatch: expected=%d actual=%d".formatted(expectedSize, size)
            );
        }

        long[] bits = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            bits[i] = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 8 + i * 8L);
        }

        return new BloomFilter(bits, numBlocks, numHashFunctions);
    }

    boolean mightContain(Slice key) {
        int hash1 = murmur3_32(key, 0);
        int hash2 = murmur3_32(key, 1);

        int blockIndex = Integer.remainderUnsigned(hash1, numBlocks);
        int blockOffset = blockIndex * CACHE_LINE_LONGS;

        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + (i + 1) * hash2;
            int bitPosition = Integer.remainderUnsigned(combinedHash, CACHE_LINE_BITS);

            int longIndex = bitPosition >>> 6;
            int bitIndex = bitPosition & 63;

            if ((bits[blockOffset + longIndex] & (1L << bitIndex)) == 0) {
                return false;
            }
        }

        return true;
    }

    byte[] serialize() {
        int size = 4 + 4 + bits.length * 8;
        ByteBuffer buffer = ByteBuffer.allocate(size).order(ByteOrder.nativeOrder());
        buffer.putInt(numHashFunctions);
        buffer.putInt(numBlocks);
        for (long word : bits) {
            buffer.putLong(word);
        }
        return buffer.array();
    }

    private static BloomFilter create(int expectedEntries, double falsePositiveRate) {
        if (expectedEntries <= 0) {
            throw new IllegalArgumentException("expectedEntries must be positive");
        }
        if (falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("falsePositiveRate must be between 0 and 1");
        }

        int bitsNeeded = optimalNumBits(expectedEntries, falsePositiveRate);
        int numBlocks = Math.max(1, (bitsNeeded + CACHE_LINE_BITS - 1) / CACHE_LINE_BITS);
        int numHashFunctions = optimalNumHashFunctions(expectedEntries, numBlocks * CACHE_LINE_BITS);

        long[] bits = new long[numBlocks * CACHE_LINE_LONGS];

        return new BloomFilter(bits, numBlocks, numHashFunctions);
    }

    private void add(Slice key) {
        int hash1 = murmur3_32(key, 0);
        int hash2 = murmur3_32(key, 1);

        int blockIndex = Integer.remainderUnsigned(hash1, numBlocks);
        int blockOffset = blockIndex * CACHE_LINE_LONGS;

        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + (i + 1) * hash2;
            int bitPosition = Integer.remainderUnsigned(combinedHash, CACHE_LINE_BITS);

            int longIndex = bitPosition >>> 6;
            int bitIndex = bitPosition & 63;

            bits[blockOffset + longIndex] |= (1L << bitIndex);
        }
    }

    private static int optimalNumBits(int expectedEntries, double falsePositiveRate) {
        double numBits = -(expectedEntries * Math.log(falsePositiveRate)) / (Math.log(2) * Math.log(2));
        return (int) Math.ceil(numBits);
    }

    private static int murmur3_32(Slice key, int seed) {
        MemorySegment segment = key.segment();
        int length = key.length();
        int hash = seed;

        int roundedEnd = length & ~3;
        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (segment.get(ValueLayout.JAVA_BYTE, i) & 0xff) |
                     ((segment.get(ValueLayout.JAVA_BYTE, i + 1) & 0xff) << 8) |
                     ((segment.get(ValueLayout.JAVA_BYTE, i + 2) & 0xff) << 16) |
                     ((segment.get(ValueLayout.JAVA_BYTE, i + 3) & 0xff) << 24);

            k1 *= MURMUR3_C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= MURMUR3_C2;

            hash ^= k1;
            hash = Integer.rotateLeft(hash, 13);
            hash = hash * 5 + 0xe6546b64;
        }

        int k1 = 0;
        int remaining = length & 3;
        if (remaining >= 3) {
            k1 = (segment.get(ValueLayout.JAVA_BYTE, roundedEnd + 2) & 0xff) << 16;
        }
        if (remaining >= 2) {
            k1 |= (segment.get(ValueLayout.JAVA_BYTE, roundedEnd + 1) & 0xff) << 8;
        }
        if (remaining >= 1) {
            k1 |= segment.get(ValueLayout.JAVA_BYTE, roundedEnd) & 0xff;
            k1 *= MURMUR3_C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= MURMUR3_C2;
            hash ^= k1;
        }

        hash ^= length;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;

        return hash;
    }

    private static int optimalNumHashFunctions(int expectedEntries, int numBits) {
        double numHashFunctions = ((double) numBits / expectedEntries) * Math.log(2);
        return Math.max(1, Math.min(30, (int) Math.round(numHashFunctions)));
    }
}
