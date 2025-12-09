package io.partdb.storage.compaction;

import io.partdb.storage.manifest.SSTableInfo;

import java.util.List;
import java.util.Objects;

public record CompactionTask(
    List<SSTableInfo> inputs,
    int targetLevel,
    boolean gcTombstones
) {

    public CompactionTask {
        Objects.requireNonNull(inputs, "inputs");
        inputs = List.copyOf(inputs);

        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs cannot be empty");
        }
        if (targetLevel < 0) {
            throw new IllegalArgumentException("targetLevel must be non-negative");
        }
    }
}
