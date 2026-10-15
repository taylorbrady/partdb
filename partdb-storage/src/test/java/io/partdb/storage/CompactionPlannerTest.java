package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionPlannerTest {

    @Test
    void levelZeroCompactionTakesTheOldestBatchAndOverlappingLevelOne() {
        var planner = planner(4, 4, 1_000_000);
        var levels = levels(
            List.of(table(5, 0, "e", "e", 10), table(4, 0, "d", "d", 10), table(3, 0, "c", "c", 10),
                table(2, 0, "b", "b", 10), table(1, 0, "a", "a", 10)),
            List.of(table(10, 1, "a", "c", 10), table(11, 1, "x", "z", 10))
        );

        List<CompactionPlanner.Task> tasks = planner.plan(levels, Set.of());

        assertEquals(1, tasks.size());
        assertEquals(Set.of(1L, 2L, 3L, 4L, 10L), ids(tasks.getFirst().inputs()));
        assertEquals(1, tasks.getFirst().targetLevel());
        assertTrue(tasks.getFirst().dropTombstones());
    }

    @Test
    void busyTablesAreNeverPlanned() {
        var planner = planner(2, 4, 1_000_000);
        var levels = levels(List.of(table(3, 0, "c", "c", 10), table(2, 0, "b", "b", 10), table(1, 0, "a", "a", 10)));

        List<CompactionPlanner.Task> tasks = planner.plan(levels, Set.of(1L));

        assertEquals(Set.of(2L, 3L), ids(tasks.getFirst().inputs()));
    }

    @Test
    void tombstonesAreKeptWhenDeeperLevelsOverlap() {
        var planner = planner(4, 5, 100);
        var levels = levels(
            List.of(),
            List.of(table(1, 1, "d", "f", 150)),
            List.of(table(2, 2, "d", "f", 50)),
            List.of(table(3, 3, "c", "e", 25), table(4, 3, "f", "g", 25), table(5, 3, "x", "z", 25))
        );

        CompactionPlanner.Task task = planner.plan(levels, Set.of()).getFirst();

        assertEquals(Set.of(1L, 2L), ids(task.inputs()));
        assertEquals(Set.of(3L, 4L), ids(task.grandparents()));
        assertFalse(task.dropTombstones());
    }

    @Test
    void doesNotCompactOutOfTheLastLevel() {
        var planner = planner(4, 3, 100);
        var levels = levels(List.of(), List.of(), List.of(table(1, 2, "a", "m", 800), table(2, 2, "n", "z", 800)));

        assertTrue(planner.plan(levels, Set.of()).isEmpty());
    }

    @Test
    void prefersTheSeedWithLeastOverlapInTheNextLevel() {
        var planner = planner(4, 4, 100);
        var levels = levels(
            List.of(),
            List.of(table(1, 1, "a", "c", 80), table(2, 1, "d", "f", 80)),
            List.of(table(3, 2, "a", "c", 500), table(4, 2, "g", "z", 50))
        );

        assertEquals(Set.of(2L), ids(planner.plan(levels, Set.of()).getFirst().inputs()));
    }

    @Test
    void ordersTasksByScore() {
        var planner = planner(4, 4, 100);
        var levels = levels(
            List.of(table(8, 0, "m", "m", 10), table(7, 0, "n", "n", 10), table(6, 0, "o", "o", 10), table(5, 0, "p", "p", 10)),
            List.of(table(4, 1, "a", "f", 150), table(3, 1, "g", "l", 150))
        );

        List<CompactionPlanner.Task> tasks = planner.plan(levels, Set.of());

        assertEquals(2, tasks.size());
        assertEquals(1, tasks.get(0).sourceLevel());
        assertEquals(0, tasks.get(1).sourceLevel());
    }

    private static CompactionPlanner planner(int levelZeroTrigger, int maxLevels, long levelBaseBytes) {
        return new CompactionPlanner(new StoreOptions.Compaction(1024, 4, levelZeroTrigger, levelBaseBytes, 10, maxLevels));
    }

    @SafeVarargs
    private static List<List<TableMetadata>> levels(List<TableMetadata>... levels) {
        return new ArrayList<>(List.of(levels));
    }

    private static TableMetadata table(long id, int level, String smallest, String largest, long size) {
        return new TableMetadata(id, level, Slice.utf8(smallest), Slice.utf8(largest), size);
    }

    private static Set<Long> ids(List<TableMetadata> tables) {
        return tables.stream().map(TableMetadata::id).collect(Collectors.toSet());
    }
}
