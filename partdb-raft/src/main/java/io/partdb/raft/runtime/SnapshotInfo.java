package io.partdb.raft.runtime;

import io.partdb.raft.SnapshotMetadata;

import java.util.Objects;

public record SnapshotInfo(SnapshotMetadata metadata, long size, int checksum) {
    public SnapshotInfo {
        Objects.requireNonNull(metadata, "metadata must not be null");
        if (size < 0) {
            throw new IllegalArgumentException("size must not be negative");
        }
    }
}
