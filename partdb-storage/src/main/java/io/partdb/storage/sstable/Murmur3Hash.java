package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;

final class Murmur3Hash {

    private static final int C1 = 0xcc9e2d51;
    private static final int C2 = 0x1b873593;

    static int hash(ByteArray key, int seed) {
        int length = key.length();
        int h1 = seed;

        int roundedEnd = length & ~3;

        for (int i = 0; i < roundedEnd; i += 4) {
            int k1 = (key.get(i) & 0xff) |
                     ((key.get(i + 1) & 0xff) << 8) |
                     ((key.get(i + 2) & 0xff) << 16) |
                     ((key.get(i + 3) & 0xff) << 24);

            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;

            h1 ^= k1;
            h1 = Integer.rotateLeft(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }

        int k1 = 0;
        int remaining = length & 3;
        if (remaining >= 3) {
            k1 = (key.get(roundedEnd + 2) & 0xff) << 16;
        }
        if (remaining >= 2) {
            k1 |= (key.get(roundedEnd + 1) & 0xff) << 8;
        }
        if (remaining >= 1) {
            k1 |= (key.get(roundedEnd) & 0xff);
            k1 *= C1;
            k1 = Integer.rotateLeft(k1, 15);
            k1 *= C2;
            h1 ^= k1;
        }

        h1 ^= length;
        h1 ^= (h1 >>> 16);
        h1 *= 0x85ebca6b;
        h1 ^= (h1 >>> 13);
        h1 *= 0xc2b2ae35;
        h1 ^= (h1 >>> 16);

        return h1;
    }
}
