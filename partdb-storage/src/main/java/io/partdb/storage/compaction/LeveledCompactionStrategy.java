package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class LeveledCompactionStrategy implements CompactionStrategy {

    private final LeveledCompactionConfig config;
    private final Map<Integer, AtomicInteger> levelRoundRobin;

    public LeveledCompactionStrategy(LeveledCompactionConfig config) {
        this.config = config;
        this.levelRoundRobin = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<CompactionTask> selectCompaction(ManifestData manifest) {
        Optional<CompactionTask> l0Task = selectL0Compaction(manifest);
        if (l0Task.isPresent()) {
            return l0Task;
        }

        for (int level = 1; level < config.maxLevels(); level++) {
            Optional<CompactionTask> task = selectLevelCompaction(manifest, level);
            if (task.isPresent()) {
                return task;
            }
        }

        return Optional.empty();
    }

    private Optional<CompactionTask> selectL0Compaction(ManifestData manifest) {
        List<SSTableMetadata> l0Files = manifest.level(0);

        if (l0Files.size() < config.l0CompactionTrigger()) {
            return Optional.empty();
        }

        List<SSTableMetadata> l1Files = manifest.level(1);
        List<SSTableMetadata> overlappingL1 = findOverlapping(l0Files, l1Files);

        List<SSTableMetadata> allInputs = new ArrayList<>();
        allInputs.addAll(l0Files);
        allInputs.addAll(overlappingL1);

        boolean isBottomLevel = 1 == config.maxLevels() - 1;
        return Optional.of(new CompactionTask(allInputs, 1, isBottomLevel));
    }

    private Optional<CompactionTask> selectLevelCompaction(ManifestData manifest, int level) {
        long currentSize = manifest.levelSize(level);
        long maxSize = config.maxBytesForLevel(level);

        if (currentSize <= maxSize) {
            return Optional.empty();
        }

        List<SSTableMetadata> levelFiles = manifest.level(level);
        if (levelFiles.isEmpty()) {
            return Optional.empty();
        }

        AtomicInteger counter = levelRoundRobin.computeIfAbsent(level, k -> new AtomicInteger(0));
        int index = Math.floorMod(counter.getAndIncrement(), levelFiles.size());
        SSTableMetadata selected = levelFiles.get(index);

        List<SSTableMetadata> nextLevelFiles = manifest.level(level + 1);
        List<SSTableMetadata> overlapping = findOverlapping(List.of(selected), nextLevelFiles);

        List<SSTableMetadata> allInputs = new ArrayList<>();
        allInputs.add(selected);
        allInputs.addAll(overlapping);

        int targetLevel = level + 1;
        boolean isBottomLevel = targetLevel == config.maxLevels() - 1;
        return Optional.of(new CompactionTask(allInputs, targetLevel, isBottomLevel));
    }

    private List<SSTableMetadata> findOverlapping(
        List<SSTableMetadata> sources,
        List<SSTableMetadata> candidates
    ) {
        if (sources.isEmpty() || candidates.isEmpty()) {
            return List.of();
        }

        ByteArray minKey = sources.stream()
            .map(SSTableMetadata::smallestKey)
            .min(ByteArray::compareTo)
            .orElseThrow();

        ByteArray maxKey = sources.stream()
            .map(SSTableMetadata::largestKey)
            .max(ByteArray::compareTo)
            .orElseThrow();

        return candidates.stream()
            .filter(sst -> sst.overlaps(minKey, maxKey))
            .toList();
    }
}
