package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftSnapshot;

import java.util.Objects;

public record StoredSnapshot(long index, long term, Bytes data) {
    public StoredSnapshot {
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must not be negative");
        }
        data = Objects.requireNonNull(data, "data must not be null");
    }

    static StoredSnapshot fromRaftSnapshot(RaftSnapshot snapshot) {
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        return new StoredSnapshot(snapshot.index(), snapshot.term(), snapshot.data());
    }
}
