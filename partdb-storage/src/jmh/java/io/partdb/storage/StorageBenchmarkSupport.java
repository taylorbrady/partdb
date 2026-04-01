package io.partdb.storage;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

final class StorageBenchmarkSupport {

    private StorageBenchmarkSupport() {
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
            case "RANDOMISH" -> Slice.copyOf(randomishKeyBytes(index));
            default -> throw new IllegalArgumentException("Unknown keyShape: " + keyShape);
        };
    }

    static Slice valueSlice(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return Slice.copyOf(bytes);
    }

    static byte[] payload(String payloadKind, int size, long seed) {
        return switch (payloadKind) {
            case "COMPRESSIBLE" -> compressiblePayload(size);
            case "SEMI_COMPRESSIBLE" -> semiCompressiblePayload(size, seed);
            case "RANDOM" -> randomPayload(size, seed);
            default -> throw new IllegalArgumentException("Unknown payloadKind: " + payloadKind);
        };
    }

    static DataBlockReader dataBlockReader(String keyShape, int entryCount, int valueSize, int restartInterval) {
        DataBlockWriter writer = new DataBlockWriter(restartInterval);
        Slice[] keys = keys(keyShape, entryCount, 0);
        for (int i = 0; i < keys.length; i++) {
            writer.append(new StoredEntry.Value(
                keys[i],
                valueSlice(valueSize, 0x5eedL + i),
                i + 1L
            ));
        }
        return DataBlockReader.from(MemorySegment.ofArray(writer.finish()));
    }

    static DataBlockReader cacheBlock(long blockSeed, int valueSize) {
        DataBlockWriter writer = new DataBlockWriter(4);
        writer.append(new StoredEntry.Value(
            Slice.copyOf(randomishKeyBytes((int) blockSeed)),
            valueSlice(valueSize, blockSeed),
            blockSeed + 1
        ));
        return DataBlockReader.from(MemorySegment.ofArray(writer.finish()));
    }

    static BloomFilter bloomFilter(String keyShape, int keyCount, double falsePositiveRate) {
        return BloomFilter.build(List.of(keys(keyShape, keyCount, 0)), falsePositiveRate);
    }

    static Slice strictlyAfter(Slice key) {
        byte[] bytes = key.toByteArray();
        byte[] extended = new byte[bytes.length + 1];
        System.arraycopy(bytes, 0, extended, 0, bytes.length);
        extended[extended.length - 1] = (byte) 0xff;
        return Slice.copyOf(extended);
    }

    static List<Slice> sliceList(Slice[] keys) {
        List<Slice> list = new ArrayList<>(keys.length);
        for (Slice key : keys) {
            list.add(key);
        }
        return list;
    }

    private static byte[] compressiblePayload(int size) {
        byte[] bytes = new byte[size];
        byte[] pattern = "partdb-block-codec-pattern-".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < size; i++) {
            bytes[i] = pattern[i % pattern.length];
        }
        return bytes;
    }

    private static byte[] semiCompressiblePayload(int size, long seed) {
        byte[] bytes = compressiblePayload(size);
        Random random = new Random(seed);
        for (int i = 0; i < bytes.length; i += 32) {
            bytes[i] = (byte) random.nextInt(256);
        }
        return bytes;
    }

    private static byte[] randomPayload(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return bytes;
    }

    private static byte[] randomishKeyBytes(int index) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);
        buffer.putLong(index);
        buffer.putLong(mix64(index ^ 0x9e3779b9));
        return buffer.array();
    }

    private static long mix64(long value) {
        long z = value + 0x9e3779b97f4a7c15L;
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
