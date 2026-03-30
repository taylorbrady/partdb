package io.partdb.storage;

import java.util.Arrays;
import java.util.Objects;

public final class StorageSnapshot {

    private final byte[] bytes;

    public StorageSnapshot(byte[] bytes) {
        this.bytes = Objects.requireNonNull(bytes, "bytes must not be null").clone();
    }

    public byte[] bytes() {
        return bytes.clone();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StorageSnapshot other)) {
            return false;
        }
        return Arrays.equals(bytes, other.bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "StorageSnapshot[" + bytes.length + " bytes]";
    }
}
