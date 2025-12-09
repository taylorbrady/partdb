package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

final class BloomFilter {

    private static final int CACHE_LINE_BITS = 512;
    private static final int CACHE_LINE_LONGS = 8;

    private final long[] bits;
    private final int numBlocks;
    private final int numHashFunctions;

    private BloomFilter(long[] bits, int numBlocks, int numHashFunctions) {
        this.bits = bits;
        this.numBlocks = numBlocks;
        this.numHashFunctions = numHashFunctions;
    }

    static BloomFilter build(List<ByteArray> keys, double falsePositiveRate) {
        if (keys.isEmpty()) {
            return create(1, falsePositiveRate);
        }
        BloomFilter filter = create(keys.size(), falsePositiveRate);
        for (ByteArray key : keys) {
            filter.add(key);
        }
        return filter;
    }

    static BloomFilter from(MemorySegment segment) {
        int numHashFunctions = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 0);
        int numBlocks = segment.get(ValueLayout.JAVA_INT_UNALIGNED, 4);
        int numLongs = numBlocks * CACHE_LINE_LONGS;

        long[] bits = new long[numLongs];
        for (int i = 0; i < numLongs; i++) {
            bits[i] = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, 8 + i * 8L);
        }

        return new BloomFilter(bits, numBlocks, numHashFunctions);
    }

    boolean mightContain(ByteArray key) {
        int hash1 = Murmur3Hash.hash(key, 0);
        int hash2 = Murmur3Hash.hash(key, 1);

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

    private void add(ByteArray key) {
        int hash1 = Murmur3Hash.hash(key, 0);
        int hash2 = Murmur3Hash.hash(key, 1);

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

    private static int optimalNumHashFunctions(int expectedEntries, int numBits) {
        double numHashFunctions = ((double) numBits / expectedEntries) * Math.log(2);
        return Math.max(1, Math.min(30, (int) Math.round(numHashFunctions)));
    }
}
