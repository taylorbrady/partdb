package io.partdb.storage.compaction;

import java.util.List;
import java.util.Objects;

public record ManifestData(
    long nextSSTableId,
    long lastAppliedIndex,
    List<SSTableMetadata> sstables
) {

    public ManifestData {
        Objects.requireNonNull(sstables, "sstables cannot be null");
        sstables = List.copyOf(sstables);

        if (nextSSTableId < 0) {
            throw new IllegalArgumentException("nextSSTableId must be non-negative");
        }
        if (lastAppliedIndex < 0) {
            throw new IllegalArgumentException("lastAppliedIndex must be non-negative");
        }
    }

    public List<SSTableMetadata> level(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .toList();
    }

    public long levelSize(int level) {
        return sstables.stream()
            .filter(sst -> sst.level() == level)
            .mapToLong(SSTableMetadata::fileSizeBytes)
            .sum();
    }

    public int maxLevel() {
        return sstables.stream()
            .mapToInt(SSTableMetadata::level)
            .max()
            .orElse(0);
    }
}
