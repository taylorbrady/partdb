package io.partdb.node.admin;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record PartDbBackup(Bytes snapshotBytes, long appliedIndex) {
    public PartDbBackup {
        snapshotBytes = Objects.requireNonNull(snapshotBytes, "snapshotBytes must not be null");
        if (appliedIndex < 0) {
            throw new IllegalArgumentException("appliedIndex must not be negative");
        }
    }
}
