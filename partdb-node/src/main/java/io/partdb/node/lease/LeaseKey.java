package io.partdb.node.lease;

import java.util.Arrays;
import java.util.Objects;

final class LeaseKey {

    private final byte[] bytes;

    private LeaseKey(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes").clone();
    }

    static LeaseKey of(byte[] bytes) {
        return new LeaseKey(bytes);
    }

    byte[] toByteArray() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LeaseKey other && Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }
}
