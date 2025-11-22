package io.partdb.storage.compaction;

import java.util.Optional;

public interface CompactionStrategy {
    Optional<CompactionTask> selectCompaction(ManifestData manifest);
}
