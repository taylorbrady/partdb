package io.partdb.storage;

import java.util.Arrays;
import java.util.Objects;

public final class VersionedEntry {

    private final byte[] key;
    private final byte[] value;
    private final long revision;

    public VersionedEntry(byte[] key, byte[] value, long revision) {
        this.key = Objects.requireNonNull(key, "key must not be null").clone();
        this.value = Objects.requireNonNull(value, "value must not be null").clone();
        this.revision = revision;
    }

    public byte[] key() {
        return key.clone();
    }

    public byte[] value() {
        return value.clone();
    }

    public long revision() {
        return revision;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof VersionedEntry other)) {
            return false;
        }
        return revision == other.revision
            && Arrays.equals(key, other.key)
            && Arrays.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Long.hashCode(revision);
        return result;
    }

    @Override
    public String toString() {
        return "VersionedEntry[key=" + key.length + " bytes, value=" + value.length + " bytes, revision=" + revision + "]";
    }
}
