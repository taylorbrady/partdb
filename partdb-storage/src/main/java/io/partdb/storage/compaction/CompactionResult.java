package io.partdb.storage.compaction;

import io.partdb.storage.sstable.SSTableDescriptor;

import java.util.List;

public sealed interface CompactionResult {

    CompactionTask task();

    record Success(CompactionTask task, List<SSTableDescriptor> outputs) implements CompactionResult {
        public Success {
            outputs = List.copyOf(outputs);
        }
    }

    record Failure(CompactionTask task, Throwable cause) implements CompactionResult {}
}
