package io.partdb.common;

import java.util.Arrays;

public record KeyBytes(byte[] data) {

    public KeyBytes {
        data = data.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof KeyBytes kb && Arrays.equals(data, kb.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "KeyBytes[" + Arrays.toString(data) + "]";
    }
}
