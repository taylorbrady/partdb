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

    List<CompactionTask> selectCompactions(SSTableManifest manifest, Set<Long> excludedSSTableIds) {
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

    private Optional<ScoredTask> l0Candidate(SSTableManifest manifest, Set<Long> excludedIds) {
        List<SSTableMetadata> l0Files = manifest.level(0).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();

        if (l0Files.size() < config.l0CompactionTrigger()) {
            return Optional.empty();
        }

        List<SSTableMetadata> l0Batch = selectL0Batch(l0Files);
        List<SSTableMetadata> l1Files = manifest.level(1).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .toList();
        List<SSTableMetadata> overlappingL1 = findOverlapping(l0Batch, l1Files);

        List<SSTableMetadata> allInputs = new ArrayList<>();
        allInputs.addAll(l0Batch);
        allInputs.addAll(overlappingL1);

        boolean gcTombstones = 1 == config.maxLevels() - 1;
        double score = (double) l0Files.size() / config.l0CompactionTrigger();
        return Optional.of(new ScoredTask(
            0,
            score,
            new CompactionTask(allInputs, grandparents(manifest, allInputs, 1), 1, gcTombstones)
        ));
    }

    private Optional<ScoredTask> levelCandidate(SSTableManifest manifest, int level, Set<Long> excludedIds) {
        long currentSize = manifest.levelSize(level);
        long maxSize = config.maxBytesForLevel(level);

        if (currentSize <= maxSize) {
            return Optional.empty();
        }

        List<SSTableMetadata> levelFiles = manifest.level(level).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .sorted(metadataOrder())
            .toList();
        if (levelFiles.isEmpty()) {
            return Optional.empty();
        }

        List<SSTableMetadata> nextLevelFiles = manifest.level(level + 1).stream()
            .filter(sst -> !excludedIds.contains(sst.id()))
            .sorted(metadataOrder())
            .toList();

        AtomicInteger counter = levelRoundRobin.computeIfAbsent(level, _ -> new AtomicInteger(0));
        int startIndex = Math.floorMod(counter.getAndIncrement(), levelFiles.size());
        SSTableMetadata selected = selectLevelSeed(levelFiles, nextLevelFiles, startIndex);
        List<SSTableMetadata> overlapping = findOverlapping(List.of(selected), nextLevelFiles);

        List<SSTableMetadata> allInputs = new ArrayList<>();
        allInputs.add(selected);
        allInputs.addAll(overlapping);

        int targetLevel = level + 1;
        boolean gcTombstones = targetLevel == config.maxLevels() - 1;
        double score = (double) currentSize / maxSize;
        return Optional.of(new ScoredTask(
            level,
            score,
            new CompactionTask(allInputs, grandparents(manifest, allInputs, targetLevel), targetLevel, gcTombstones)
        ));
    }

    private List<SSTableMetadata> findOverlapping(
        List<SSTableMetadata> sources,
        List<SSTableMetadata> candidates
    ) {
        if (sources.isEmpty() || candidates.isEmpty()) {
            return List.of();
        }

        Slice minKey = sources.stream()
            .map(SSTableMetadata::smallestKey)
            .min(Comparator.naturalOrder())
            .orElseThrow();

        Slice maxKey = sources.stream()
            .map(SSTableMetadata::largestKey)
            .max(Comparator.naturalOrder())
            .orElseThrow();

        return candidates.stream()
            .filter(sst -> sst.overlaps(minKey, maxKey))
            .toList();
    }

    private List<SSTableMetadata> selectL0Batch(List<SSTableMetadata> l0Files) {
        int batchSize = Math.min(l0Files.size(), config.l0CompactionTrigger());
        return l0Files.subList(l0Files.size() - batchSize, l0Files.size());
    }

    private static boolean conflicts(CompactionTask task, Set<Long> usedIds) {
        return task.inputs().stream().anyMatch(sst -> usedIds.contains(sst.id()));
    }

    private List<SSTableMetadata> grandparents(
        SSTableManifest manifest,
        List<SSTableMetadata> inputs,
        int targetLevel
    ) {
        int grandparentLevel = targetLevel + 1;
        if (grandparentLevel >= config.maxLevels()) {
            return List.of();
        }
        List<SSTableMetadata> grandparentFiles = manifest.level(grandparentLevel).stream()
            .sorted(metadataOrder())
            .toList();
        return findOverlapping(inputs, grandparentFiles);
    }

    private SSTableMetadata selectLevelSeed(
        List<SSTableMetadata> levelFiles,
        List<SSTableMetadata> nextLevelFiles,
        int startIndex
    ) {
        if (levelFiles.size() == 1 || nextLevelFiles.isEmpty()) {
            return levelFiles.get(startIndex);
        }

        CandidateChoice best = null;

        for (int offset = 0; offset < levelFiles.size(); offset++) {
            SSTableMetadata candidate = levelFiles.get((startIndex + offset) % levelFiles.size());
            List<SSTableMetadata> overlapping = findOverlapping(List.of(candidate), nextLevelFiles);
            long overlapBytes = overlapping.stream()
                .mapToLong(SSTableMetadata::fileSizeBytes)
                .sum();

            CandidateChoice choice = new CandidateChoice(
                candidate,
                overlapBytes,
                overlapping.size(),
                offset
            );

            if (best == null || choice.betterThan(best)) {
                best = choice;
            }
        }

        return best.metadata();
    }

    private static Comparator<SSTableMetadata> metadataOrder() {
        return Comparator
            .comparing(SSTableMetadata::smallestKey)
            .thenComparing(SSTableMetadata::largestKey)
            .thenComparingLong(SSTableMetadata::id);
    }

    private record ScoredTask(int sourceLevel, double score, CompactionTask task) {}

    private record CandidateChoice(
        SSTableMetadata metadata,
        long overlapBytes,
        int overlapCount,
        int rotationDistance
    ) {
        boolean betterThan(CandidateChoice other) {
            return overlapBytes < other.overlapBytes
                || (overlapBytes == other.overlapBytes && overlapCount < other.overlapCount)
                || (overlapBytes == other.overlapBytes
                && overlapCount == other.overlapCount
                && rotationDistance < other.rotationDistance);
        }
    }
}
