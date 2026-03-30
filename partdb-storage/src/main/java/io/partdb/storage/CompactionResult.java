package io.partdb.storage;

import java.util.List;

sealed interface CompactionResult {

    CompactionTask task();

    record Success(CompactionTask task, List<SSTableMetadata> outputs) implements CompactionResult {
        public Success {
            outputs = List.copyOf(outputs);
        }
    }

    record Failure(CompactionTask task, Throwable cause) implements CompactionResult {}
}
