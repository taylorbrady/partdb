package io.partdb.storage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

final class CompactionPlanner {

    static final int LEVEL_MULTIPLIER = 10;
    static final int GRANDPARENT_OVERLAP_FACTOR = 10;

    sealed interface Task {
        List<TableMetadata> inputs();

        int sourceLevel();

        int targetLevel();

        double score();
    }

    record Move(TableMetadata table, int sourceLevel, int targetLevel, double score) implements Task {
        @Override
        public List<TableMetadata> inputs() {
            return List.of(table);
        }
    }

    record Merge(
        List<TableMetadata> inputs,
        List<TableMetadata> grandparents,
        int sourceLevel,
        int targetLevel,
        boolean dropTombstones,
        double score
    ) implements Task {
        Merge {
            inputs = List.copyOf(inputs);
            grandparents = List.copyOf(grandparents);
        }
    }

    private final int levelZeroTrigger;
    private final long levelBaseBytes;
    private final long maxGrandparentOverlap;

    CompactionPlanner(int levelZeroTrigger, long levelBaseBytes, long targetTableBytes) {
        this.levelZeroTrigger = levelZeroTrigger;
        this.levelBaseBytes = levelBaseBytes;
        this.maxGrandparentOverlap = targetTableBytes * GRANDPARENT_OVERLAP_FACTOR;
    }

    List<Task> plan(List<List<TableMetadata>> levels, Set<Long> busy) {
        int bottom = levels.size() - 1;
        int base = baseLevel(levels);
        List<Task> candidates = new ArrayList<>();
        levelZero(levels, base, busy).ifPresent(candidates::add);
        for (int level = base; level < bottom; level++) {
            deeper(levels, level, targetBytes(levels, level), busy).ifPresent(candidates::add);
        }
        candidates.sort(Comparator.comparingDouble(Task::score).reversed());

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

    int baseLevel(List<List<TableMetadata>> levels) {
        int bottom = levels.size() - 1;
        int base = bottom;
        for (int level = 1; level < bottom; level++) {
            if (!levels.get(level).isEmpty()) {
                base = level;
                break;
            }
        }
        long target = largestLevelBytes(levels);
        for (int level = bottom; level > base; level--) {
            target /= LEVEL_MULTIPLIER;
        }
        while (base > 1 && target > levelBaseBytes) {
            base--;
            target /= LEVEL_MULTIPLIER;
        }
        return base;
    }

    long targetBytes(List<List<TableMetadata>> levels, int level) {
        long target = largestLevelBytes(levels);
        for (int i = levels.size() - 1; i > level; i--) {
            target /= LEVEL_MULTIPLIER;
        }
        return Math.max(target, levelBaseBytes);
    }

    private Optional<Task> levelZero(List<List<TableMetadata>> levels, int base, Set<Long> busy) {
        List<TableMetadata> files = levels.getFirst();
        if (files.size() < levelZeroTrigger || anyBusy(files, busy)) {
            return Optional.empty();
        }
        return task(levels, files, 0, base, (double) files.size() / levelZeroTrigger, busy);
    }

    private Optional<Task> deeper(List<List<TableMetadata>> levels, int level, long target, Set<Long> busy) {
        List<TableMetadata> files = levels.get(level);
        long size = bytes(files);
        if (size <= target) {
            return Optional.empty();
        }
        List<TableMetadata> next = levels.get(level + 1);
        TableMetadata seed = null;
        double seedRatio = Double.MAX_VALUE;
        for (TableMetadata file : files) {
            List<TableMetadata> overlap = overlapping(List.of(file), next);
            if (busy.contains(file.id()) || anyBusy(overlap, busy)) {
                continue;
            }
            double ratio = (double) bytes(overlap) / Math.max(1, file.sizeBytes());
            if (ratio < seedRatio) {
                seed = file;
                seedRatio = ratio;
            }
        }
        return seed == null ? Optional.empty() : task(levels, List.of(seed), level, level + 1, (double) size / target, busy);
    }

    private Optional<Task> task(List<List<TableMetadata>> levels, List<TableMetadata> sources, int sourceLevel, int targetLevel, double score, Set<Long> busy) {
        List<TableMetadata> overlap = overlapping(sources, levels.get(targetLevel));
        if (anyBusy(overlap, busy)) {
            return Optional.empty();
        }
        List<TableMetadata> inputs = new ArrayList<>(sources);
        inputs.addAll(overlap);
        List<TableMetadata> grandparents = targetLevel + 1 < levels.size() ? overlapping(inputs, levels.get(targetLevel + 1)) : List.of();
        boolean bottommost = bottommost(levels, inputs, targetLevel);
        if (sources.size() == 1 && overlap.isEmpty() && bytes(grandparents) <= maxGrandparentOverlap
            && (!bottommost || sources.getFirst().tombstones() == 0)) {
            return Optional.of(new Move(sources.getFirst(), sourceLevel, targetLevel, score));
        }
        return Optional.of(new Merge(inputs, grandparents, sourceLevel, targetLevel, bottommost, score));
    }

    private static boolean bottommost(List<List<TableMetadata>> levels, List<TableMetadata> inputs, int targetLevel) {
        for (int level = targetLevel + 1; level < levels.size(); level++) {
            if (!overlapping(inputs, levels.get(level)).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static long largestLevelBytes(List<List<TableMetadata>> levels) {
        long largest = 0;
        for (int level = 1; level < levels.size(); level++) {
            largest = Math.max(largest, bytes(levels.get(level)));
        }
        return largest;
    }

    private static long bytes(List<TableMetadata> tables) {
        return tables.stream().mapToLong(TableMetadata::sizeBytes).sum();
    }

    private static boolean anyBusy(List<TableMetadata> tables, Set<Long> busy) {
        return tables.stream().anyMatch(table -> busy.contains(table.id()));
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
