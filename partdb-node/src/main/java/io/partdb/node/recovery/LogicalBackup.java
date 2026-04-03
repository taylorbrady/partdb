package io.partdb.node.recovery;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record LogicalBackup(Bytes snapshotBytes, long appliedIndex) {
    public LogicalBackup {
        snapshotBytes = Objects.requireNonNull(snapshotBytes, "snapshotBytes must not be null");
        if (appliedIndex < 0) {
            throw new IllegalArgumentException("appliedIndex must not be negative");
        }
    }
}
