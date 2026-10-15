package io.partdb.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class CompactionPlanner {

    record Task(
        List<TableMetadata> inputs,
        List<TableMetadata> grandparents,
        int sourceLevel,
        int targetLevel,
        boolean dropTombstones,
        double score
    ) {
        Task {
            inputs = List.copyOf(inputs);
            grandparents = List.copyOf(grandparents);
        }

        Slice smallest() {
            return inputs.stream().map(TableMetadata::smallest).min(Comparator.naturalOrder()).orElseThrow();
        }

        Slice largest() {
            return inputs.stream().map(TableMetadata::largest).max(Comparator.naturalOrder()).orElseThrow();
        }
    }

    private final StoreOptions.Compaction options;
    private final Map<Integer, Integer> cursors = new HashMap<>();

    CompactionPlanner(StoreOptions.Compaction options) {
        this.options = options;
    }

    List<Task> plan(List<List<TableMetadata>> levels, Set<Long> busy) {
        if (options.maxLevels() <= 1) {
            return List.of();
        }
        List<Task> candidates = new ArrayList<>();
        levelZero(levels, busy).ifPresent(candidates::add);
        for (int level = 1; level < options.maxLevels() - 1 && level < levels.size(); level++) {
            deeper(levels, level, busy).ifPresent(candidates::add);
        }
        candidates.sort(Comparator.comparingDouble(Task::score).reversed().thenComparingInt(Task::sourceLevel));

        List<Task> tasks = new ArrayList<>();
        Set<Long> used = new HashSet<>(busy);
        for (Task task : candidates) {
            if (task.inputs().stream().noneMatch(input -> used.contains(input.id()))) {
                tasks.add(task);
                task.inputs().forEach(input -> used.add(input.id()));
            }
        }
        return tasks;
    }

    private Optional<Task> levelZero(List<List<TableMetadata>> levels, Set<Long> busy) {
        List<TableMetadata> files = available(levels, 0, busy);
        if (files.size() < options.levelZeroTrigger()) {
            return Optional.empty();
        }
        List<TableMetadata> batch = files.subList(files.size() - options.levelZeroTrigger(), files.size());
        return Optional.of(task(levels, batch, 0, (double) files.size() / options.levelZeroTrigger(), busy));
    }

    private Optional<Task> deeper(List<List<TableMetadata>> levels, int level, Set<Long> busy) {
        long size = levels.get(level).stream().mapToLong(TableMetadata::sizeBytes).sum();
        long limit = options.maxBytesForLevel(level);
        List<TableMetadata> files = available(levels, level, busy);
        if (size <= limit || files.isEmpty()) {
            return Optional.empty();
        }
        List<TableMetadata> next = available(levels, level + 1, busy);
        int start = Math.floorMod(cursors.merge(level, 1, Integer::sum) - 1, files.size());
        TableMetadata seed = files.get(start);
        long seedOverlap = Long.MAX_VALUE;
        for (int offset = 0; offset < files.size(); offset++) {
            TableMetadata candidate = files.get((start + offset) % files.size());
            long overlap = overlapping(List.of(candidate), next).stream().mapToLong(TableMetadata::sizeBytes).sum();
            if (overlap < seedOverlap) {
                seed = candidate;
                seedOverlap = overlap;
            }
        }
        return Optional.of(task(levels, List.of(seed), level, (double) size / limit, busy));
    }

    private Task task(List<List<TableMetadata>> levels, List<TableMetadata> sources, int sourceLevel, double score, Set<Long> busy) {
        int targetLevel = sourceLevel + 1;
        List<TableMetadata> inputs = new ArrayList<>(sources);
        inputs.addAll(overlapping(sources, available(levels, targetLevel, busy)));
        List<TableMetadata> grandparents = targetLevel + 1 < levels.size()
            ? overlapping(inputs, levels.get(targetLevel + 1))
            : List.of();
        return new Task(inputs, grandparents, sourceLevel, targetLevel, isBottom(levels, inputs, targetLevel), score);
    }

    private static boolean isBottom(List<List<TableMetadata>> levels, List<TableMetadata> inputs, int targetLevel) {
        for (int level = targetLevel + 1; level < levels.size(); level++) {
            if (!overlapping(inputs, levels.get(level)).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static List<TableMetadata> available(List<List<TableMetadata>> levels, int level, Set<Long> busy) {
        if (level >= levels.size()) {
            return List.of();
        }
        return levels.get(level).stream().filter(table -> !busy.contains(table.id())).toList();
    }

    private static List<TableMetadata> overlapping(List<TableMetadata> sources, List<TableMetadata> candidates) {
        if (sources.isEmpty() || candidates.isEmpty()) {
            return List.of();
        }
        Slice smallest = sources.stream().map(TableMetadata::smallest).min(Comparator.naturalOrder()).orElseThrow();
        Slice largest = sources.stream().map(TableMetadata::largest).max(Comparator.naturalOrder()).orElseThrow();
        return candidates.stream().filter(table -> table.overlaps(smallest, largest)).toList();
    }
}
