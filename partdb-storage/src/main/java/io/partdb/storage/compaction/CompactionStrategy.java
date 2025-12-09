package io.partdb.storage.compaction;

import io.partdb.storage.manifest.Manifest;

import java.util.Optional;

@FunctionalInterface
public interface CompactionStrategy {
    Optional<CompactionTask> selectCompaction(Manifest manifest);
}
