package io.partdb.storage;

import java.util.List;
import java.util.Objects;

record CompactionTask(
    List<SSTableMetadata> inputs,
    List<SSTableMetadata> grandparents,
    int targetLevel,
    boolean gcTombstones
) {

    CompactionTask(List<SSTableMetadata> inputs, int targetLevel, boolean gcTombstones) {
        this(inputs, List.of(), targetLevel, gcTombstones);
    }

    public CompactionTask {
        Objects.requireNonNull(inputs, "inputs");
        Objects.requireNonNull(grandparents, "grandparents");
        inputs = List.copyOf(inputs);
        grandparents = List.copyOf(grandparents);

        if (inputs.isEmpty()) {
            throw new IllegalArgumentException("inputs cannot be empty");
        }
        if (targetLevel < 0) {
            throw new IllegalArgumentException("targetLevel must be non-negative");
        }
    }
}
