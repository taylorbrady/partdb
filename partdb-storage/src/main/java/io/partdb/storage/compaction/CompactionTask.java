package io.partdb.storage.compaction;

import io.partdb.storage.sstable.SSTableDescriptor;

import java.util.List;
import java.util.Objects;

public record CompactionTask(
    List<SSTableDescriptor> inputs,
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
