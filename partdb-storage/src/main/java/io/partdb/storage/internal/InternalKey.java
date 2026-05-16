package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.Objects;

record InternalKey(Slice userKey, long revision) implements Comparable<InternalKey> {

    InternalKey {
        Objects.requireNonNull(userKey, "userKey");
        if (revision < 0) {
            throw new IllegalArgumentException("revision must be non-negative");
        }
    }

    static InternalKey firstForUser(Slice userKey) {
        return new InternalKey(userKey, Long.MAX_VALUE);
    }

    static InternalKey visibleAt(Slice userKey, long snapshotRevision) {
        return new InternalKey(userKey, snapshotRevision);
    }

    @Override
    public int compareTo(InternalKey other) {
        int keyComparison = userKey.compareTo(other.userKey);
        if (keyComparison != 0) {
            return keyComparison;
        }
        return Long.compare(other.revision, revision);
    }
}
