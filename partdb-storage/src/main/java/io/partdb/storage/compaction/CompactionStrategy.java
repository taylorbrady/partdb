package io.partdb.storage.compaction;

import io.partdb.storage.manifest.Manifest;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface CompactionStrategy {

    Optional<CompactionTask> selectCompaction(Manifest manifest);

    default List<CompactionTask> selectCompactions(Manifest manifest, Set<Long> excludedSSTableIds) {
        return selectCompaction(manifest)
            .filter(task -> task.inputs().stream()
                .noneMatch(sst -> excludedSSTableIds.contains(sst.id())))
            .map(List::of)
            .orElse(List.of());
    }
}
