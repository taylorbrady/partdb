package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;
import io.partdb.storage.manifest.Manifest;
import io.partdb.storage.manifest.SSTableInfo;

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
    public Optional<CompactionTask> selectCompaction(Manifest manifest) {
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

    private Optional<CompactionTask> selectL0Compaction(Manifest manifest) {
        List<SSTableInfo> l0Files = manifest.level(0);

        if (l0Files.size() < config.l0CompactionTrigger()) {
            return Optional.empty();
        }

        List<SSTableInfo> l1Files = manifest.level(1);
        List<SSTableInfo> overlappingL1 = findOverlapping(l0Files, l1Files);

        List<SSTableInfo> allInputs = new ArrayList<>();
        allInputs.addAll(l0Files);
        allInputs.addAll(overlappingL1);

        boolean gcTombstones = 1 == config.maxLevels() - 1;
        return Optional.of(new CompactionTask(allInputs, 1, gcTombstones));
    }

    private Optional<CompactionTask> selectLevelCompaction(Manifest manifest, int level) {
        long currentSize = manifest.levelSize(level);
        long maxSize = config.maxBytesForLevel(level);

        if (currentSize <= maxSize) {
            return Optional.empty();
        }

        List<SSTableInfo> levelFiles = manifest.level(level);
        if (levelFiles.isEmpty()) {
            return Optional.empty();
        }

        AtomicInteger counter = levelRoundRobin.computeIfAbsent(level, _ -> new AtomicInteger(0));
        int index = Math.floorMod(counter.getAndIncrement(), levelFiles.size());
        SSTableInfo selected = levelFiles.get(index);

        List<SSTableInfo> nextLevelFiles = manifest.level(level + 1);
        List<SSTableInfo> overlapping = findOverlapping(List.of(selected), nextLevelFiles);

        List<SSTableInfo> allInputs = new ArrayList<>();
        allInputs.add(selected);
        allInputs.addAll(overlapping);

        int targetLevel = level + 1;
        boolean gcTombstones = targetLevel == config.maxLevels() - 1;
        return Optional.of(new CompactionTask(allInputs, targetLevel, gcTombstones));
    }

    private List<SSTableInfo> findOverlapping(
        List<SSTableInfo> sources,
        List<SSTableInfo> candidates
    ) {
        if (sources.isEmpty() || candidates.isEmpty()) {
            return List.of();
        }

        ByteArray minKey = sources.stream()
            .map(SSTableInfo::smallestKey)
            .min(ByteArray::compareTo)
            .orElseThrow();

        ByteArray maxKey = sources.stream()
            .map(SSTableInfo::largestKey)
            .max(ByteArray::compareTo)
            .orElseThrow();

        return candidates.stream()
            .filter(sst -> sst.overlaps(minKey, maxKey))
            .toList();
    }
}
