package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;

import java.util.Random;

public final class BenchmarkValues {

    private BenchmarkValues() {
    }

    public static Bytes fixedValue(int size, long seed) {
        byte[] bytes = new byte[size];
        new Random(seed).nextBytes(bytes);
        return Bytes.copyOf(bytes);
    }
}
