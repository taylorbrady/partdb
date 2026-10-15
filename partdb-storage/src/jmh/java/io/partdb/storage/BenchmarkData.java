package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

final class BenchmarkData {

    private BenchmarkData() {
    }

    static Slice[] keys(String keyShape, int count, int startIndex) {
        Slice[] keys = new Slice[count];
        for (int i = 0; i < count; i++) {
            keys[i] = key(keyShape, startIndex + i);
        }
        return keys;
    }

    static Slice key(String keyShape, int index) {
        return switch (keyShape) {
            case "SHARED_PREFIX" -> Slice.utf8("tenant-000042/session-0007/key-%08d".formatted(index));
            case "RANDOMISH" -> Slice.wrap(randomishKey(index));
            default -> throw new IllegalArgumentException("Unknown keyShape: " + keyShape);
        };
    }

    static byte[] payload(String payloadKind, int size, long seed) {
        return switch (payloadKind) {
            case "COMPRESSIBLE" -> compressible(size);
            case "SEMI_COMPRESSIBLE" -> semiCompressible(size, seed);
            case "RANDOM" -> random(size, seed);
            default -> throw new IllegalArgumentException("Unknown payloadKind: " + payloadKind);
        };
    }

    static Block block(String keyShape, int entryCount, int valueSize, int restartInterval) {
        Slice[] keys = keys(keyShape, entryCount, 0);
        Arrays.sort(keys);
        BlockWriter writer = new BlockWriter(restartInterval);
        for (int i = 0; i < keys.length; i++) {
            writer.add(new InternalEntry.Value(new InternalKey(keys[i], i + 1L), Slice.wrap(random(valueSize, 0x5eedL + i))));
        }
        return Block.from(MemorySegment.ofArray(writer.finish()));
    }

    static Block cacheBlock(long seed, int valueSize) {
        BlockWriter writer = new BlockWriter(4);
        writer.add(new InternalEntry.Value(new InternalKey(Slice.wrap(randomishKey((int) seed)), seed + 1), Slice.wrap(random(valueSize, seed))));
        return Block.from(MemorySegment.ofArray(writer.finish()));
    }

    static BloomFilter bloomFilter(Slice[] keys, double falsePositiveRate) {
        return BloomFilter.build(List.of(keys), falsePositiveRate);
    }

    static Slice after(Slice key) {
        byte[] bytes = Arrays.copyOf(key.toByteArray(), key.length() + 1);
        bytes[bytes.length - 1] = (byte) 0xff;
        return Slice.wrap(bytes);
    }

    private static byte[] compressible(int size) {
        byte[] pattern = "partdb-block-codec-pattern-".getBytes(StandardCharsets.US_ASCII);
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = pattern[i % pattern.length];
        }
        return bytes;
    }

    private static byte[] semiCompressible(int size, long seed) {
        byte[] bytes = compressible(size);
        Random random = new Random(seed);
        for (int i = 0; i < bytes.length; i += 32) {
            bytes[i] = (byte) random.nextInt(256);
        }
        return bytes;
    }

    private static byte[] random(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static byte[] randomishKey(int index) {
        return ByteBuffer.allocate(Long.BYTES * 2).putLong(index).putLong(mix64(index ^ 0x9e3779b9)).array();
    }

    private static long mix64(long value) {
        long z = value + 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
