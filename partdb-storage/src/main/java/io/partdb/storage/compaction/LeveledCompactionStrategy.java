package io.partdb.storage.compaction;

import io.partdb.common.Slice;
import io.partdb.storage.manifest.Manifest;
import io.partdb.storage.sstable.SSTableConfig;
import io.partdb.storage.sstable.SSTableDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public final class LeveledCompactionStrategy implements CompactionStrategy {

    private final SSTableConfig config;
    private final Map<Integer, AtomicInteger> levelRoundRobin;

    public LeveledCompactionStrategy(SSTableConfig config) {
        this.config = config;
        this.levelRoundRobin = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<CompactionTask> selectCompaction(Manifest manifest) {
        Optional<CompactionTask> l0Task = selectL0Compaction(manifest, Set.of());
        if (l0Task.isPresent()) {
            return l0Task;
        }

        for (int level = 1; level < config.maxLevels(); level++) {
            Optional<CompactionTask> task = selectLevelCompaction(manifest, level, Set.of());
            if (task.isPresent()) {
                return task;
            }
        }

        return Optional.empty();
    }

    @Override
    public List<CompactionTask> selectCompactions(Manifest manifest, Set<Long> excludedSSTableIds) {
        List<CompactionTask> tasks = new ArrayList<>();
        Set<Long> usedIds = new HashSet<>(excludedSSTableIds);

        selectL0Compaction(manifest, usedIds).ifPresent(task -> {
            tasks.add(task);
            task.inputs().forEach(sst -> usedIds.add(sst.id()));
        });

        for (int level = 1; level < config.maxLevels(); level++) {
            selectLevelCompaction(manifest, level, usedIds).ifPresent(task -> {
                tasks.add(task);
                task.inputs().forEach(sst -> usedIds.add(sst.id()));
            });
        }

        return tasks;
    }

    private Optional<CompactionTask> selectL0Compaction(Manifest manifest, Set<Long> excludedIds) {
        List<SSTableDescriptor> l0Files = manifest.level(0).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();

        if (l0Files.size() < config.l0CompactionTrigger()) {
            return Optional.empty();
        }

        List<SSTableDescriptor> l1Files = manifest.level(1).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();
        List<SSTableDescriptor> overlappingL1 = findOverlapping(l0Files, l1Files);

        List<SSTableDescriptor> allInputs = new ArrayList<>();
        allInputs.addAll(l0Files);
        allInputs.addAll(overlappingL1);

        boolean gcTombstones = 1 == config.maxLevels() - 1;
        return Optional.of(new CompactionTask(allInputs, 1, gcTombstones));
    }

    private Optional<CompactionTask> selectLevelCompaction(Manifest manifest, int level, Set<Long> excludedIds) {
        long currentSize = manifest.levelSize(level);
        long maxSize = config.maxBytesForLevel(level);

        if (currentSize <= maxSize) {
            return Optional.empty();
        }

        List<SSTableDescriptor> levelFiles = manifest.level(level).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();
        if (levelFiles.isEmpty()) {
            return Optional.empty();
        }

        AtomicInteger counter = levelRoundRobin.computeIfAbsent(level, _ -> new AtomicInteger(0));
        int index = Math.floorMod(counter.getAndIncrement(), levelFiles.size());
        SSTableDescriptor selected = levelFiles.get(index);

        List<SSTableDescriptor> nextLevelFiles = manifest.level(level + 1).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();
        List<SSTableDescriptor> overlapping = findOverlapping(List.of(selected), nextLevelFiles);

        List<SSTableDescriptor> allInputs = new ArrayList<>();
        allInputs.add(selected);
        allInputs.addAll(overlapping);

        int targetLevel = level + 1;
        boolean gcTombstones = targetLevel == config.maxLevels() - 1;
        return Optional.of(new CompactionTask(allInputs, targetLevel, gcTombstones));
    }

    private List<SSTableDescriptor> findOverlapping(
        List<SSTableDescriptor> sources,
        List<SSTableDescriptor> candidates
    ) {
        if (sources.isEmpty() || candidates.isEmpty()) {
            return List.of();
        }

        Slice minKey = sources.stream()
            .map(SSTableDescriptor::smallestKey)
            .min(Comparator.naturalOrder())
            .orElseThrow();

        Slice maxKey = sources.stream()
            .map(SSTableDescriptor::largestKey)
            .max(Comparator.naturalOrder())
            .orElseThrow();

        return candidates.stream()
            .filter(sst -> sst.overlaps(minKey, maxKey))
            .toList();
    }
}
