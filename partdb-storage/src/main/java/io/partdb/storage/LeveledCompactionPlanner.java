package io.partdb.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

final class LeveledCompactionPlanner {

    private final LSMConfig config;
    private final Map<Integer, AtomicInteger> levelRoundRobin;

    LeveledCompactionPlanner(LSMConfig config) {
        this.config = config;
        this.levelRoundRobin = new ConcurrentHashMap<>();
    }

    List<CompactionTask> selectCompactions(Manifest manifest, Set<Long> excludedSSTableIds) {
        if (config.maxLevels() <= 1) {
            return List.of();
        }

        List<ScoredTask> candidates = new ArrayList<>();
        l0Candidate(manifest, excludedSSTableIds).ifPresent(candidates::add);

        for (int level = 1; level < config.maxLevels() - 1; level++) {
            levelCandidate(manifest, level, excludedSSTableIds).ifPresent(candidates::add);
        }

        candidates.sort(Comparator
            .comparingDouble(ScoredTask::score)
            .reversed()
            .thenComparingInt(ScoredTask::sourceLevel));

        List<CompactionTask> tasks = new ArrayList<>();
        Set<Long> usedIds = new HashSet<>(excludedSSTableIds);

        for (ScoredTask candidate : candidates) {
            if (conflicts(candidate.task(), usedIds)) {
                continue;
            }
            tasks.add(candidate.task());
            candidate.task().inputs().forEach(sst -> usedIds.add(sst.id()));
        }

        return tasks;
    }

    private Optional<ScoredTask> l0Candidate(Manifest manifest, Set<Long> excludedIds) {
        List<SSTableDescriptor> l0Files = manifest.level(0).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();

        if (l0Files.size() < config.l0CompactionTrigger()) {
            return Optional.empty();
        }

        List<SSTableDescriptor> l0Batch = selectL0Batch(l0Files);
        List<SSTableDescriptor> l1Files = manifest.level(1).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();
        List<SSTableDescriptor> overlappingL1 = findOverlapping(l0Batch, l1Files);

        List<SSTableDescriptor> allInputs = new ArrayList<>();
        allInputs.addAll(l0Batch);
        allInputs.addAll(overlappingL1);

        boolean gcTombstones = 1 == config.maxLevels() - 1;
        double score = (double) l0Files.size() / config.l0CompactionTrigger();
        return Optional.of(new ScoredTask(0, score, new CompactionTask(allInputs, 1, gcTombstones)));
    }

    private Optional<ScoredTask> levelCandidate(Manifest manifest, int level, Set<Long> excludedIds) {
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
        double score = (double) currentSize / maxSize;
        return Optional.of(new ScoredTask(level, score, new CompactionTask(allInputs, targetLevel, gcTombstones)));
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

    private List<SSTableDescriptor> selectL0Batch(List<SSTableDescriptor> l0Files) {
        int batchSize = Math.min(l0Files.size(), config.l0CompactionTrigger());
        return l0Files.subList(l0Files.size() - batchSize, l0Files.size());
    }

    private static boolean conflicts(CompactionTask task, Set<Long> usedIds) {
        return task.inputs().stream().anyMatch(sst -> usedIds.contains(sst.id()));
    }

    private record ScoredTask(int sourceLevel, double score, CompactionTask task) {}
}
