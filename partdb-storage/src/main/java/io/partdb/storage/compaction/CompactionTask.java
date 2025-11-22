package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;

import java.util.List;
import java.util.Objects;

public record CompactionTask(
    List<SSTableMetadata> inputs,
    int targetLevel
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

    public ByteArray smallestKey() {
        return inputs.stream()
            .map(SSTableMetadata::smallestKey)
            .min(ByteArray::compareTo)
            .orElseThrow();
    }

    public ByteArray largestKey() {
        return inputs.stream()
            .map(SSTableMetadata::largestKey)
            .max(ByteArray::compareTo)
            .orElseThrow();
    }
}
