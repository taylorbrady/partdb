package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;

import java.nio.ByteBuffer;

final class BloomFilter {

    private final byte[] bits;
    private final int numHashFunctions;

    private BloomFilter(byte[] bits, int numHashFunctions) {
        this.bits = bits;
        this.numHashFunctions = numHashFunctions;
    }

    static BloomFilter create(int expectedEntries, double falsePositiveRate) {
        if (expectedEntries <= 0) {
            throw new IllegalArgumentException("expectedEntries must be positive");
        }
        if (falsePositiveRate <= 0 || falsePositiveRate >= 1) {
            throw new IllegalArgumentException("falsePositiveRate must be between 0 and 1");
        }

        int numBits = optimalNumBits(expectedEntries, falsePositiveRate);
        int numHashFunctions = optimalNumHashFunctions(expectedEntries, numBits);

        int numBytes = (numBits + 7) / 8;
        byte[] bits = new byte[numBytes];

        return new BloomFilter(bits, numHashFunctions);
    }

    void add(ByteArray key) {
        int hash1 = Murmur3Hash.hash(key, 0);
        int hash2 = Murmur3Hash.hash(key, 1);

        int bitArraySize = bits.length * 8;

        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            int bitPosition = Math.abs(combinedHash % bitArraySize);

            int byteIndex = bitPosition / 8;
            int bitIndex = bitPosition % 8;

            bits[byteIndex] |= (1 << bitIndex);
        }
    }

    boolean mightContain(ByteArray key) {
        int hash1 = Murmur3Hash.hash(key, 0);
        int hash2 = Murmur3Hash.hash(key, 1);

        int bitArraySize = bits.length * 8;

        for (int i = 0; i < numHashFunctions; i++) {
            int combinedHash = hash1 + i * hash2;
            int bitPosition = Math.abs(combinedHash % bitArraySize);

            int byteIndex = bitPosition / 8;
            int bitIndex = bitPosition % 8;

            if ((bits[byteIndex] & (1 << bitIndex)) == 0) {
                return false;
            }
        }

        return true;
    }

    byte[] serialize() {
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + bits.length);
        buffer.putInt(numHashFunctions);
        buffer.putInt(bits.length);
        buffer.put(bits);
        return buffer.array();
    }

    static BloomFilter deserialize(ByteBuffer buffer) {
        int numHashFunctions = buffer.getInt();
        int bitsLength = buffer.getInt();
        byte[] bits = new byte[bitsLength];
        buffer.get(bits);
        return new BloomFilter(bits, numHashFunctions);
    }

    private static int optimalNumBits(int expectedEntries, double falsePositiveRate) {
        double numBits = -(expectedEntries * Math.log(falsePositiveRate)) / (Math.log(2) * Math.log(2));
        return (int) Math.ceil(numBits);
    }

    private static int optimalNumHashFunctions(int expectedEntries, int numBits) {
        double numHashFunctions = ((double) numBits / expectedEntries) * Math.log(2);
        return Math.max(1, (int) Math.round(numHashFunctions));
    }

    int sizeInBytes() {
        return bits.length;
    }
}
