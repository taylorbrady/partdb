package io.partdb.storage.compaction;

import java.util.List;
import java.util.Objects;

public record CompactionTask(
    List<SSTableMetadata> inputs,
    int targetLevel,
    boolean isBottomLevel
) {

    public CompactionTask {
        Objects.requireNonNull(inputs, "inputs cannot be null");
        inputs = List.copyOf(inputs);

        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs cannot be empty");
        }
        if (targetLevel < 0) {
            throw new IllegalArgumentException("targetLevel must be non-negative");
        }
    }
}
