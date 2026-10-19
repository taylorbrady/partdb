package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CompactionPlannerTest {

    private static final long BASE = 100;
    private final CompactionPlanner planner = new CompactionPlanner(4, BASE, 1_000);

    @Test
    void emptyStoreCompactsLevelZeroStraightIntoTheBottomLevel() {
        var levels = levels(List.of(table(4, "d", "d", 10), table(3, "c", "c", 10), table(2, "b", "b", 10), table(1, "a", "a", 10)));

        CompactionPlanner.Merge merge = assertInstanceOf(CompactionPlanner.Merge.class, planner.plan(levels, Set.of()).getFirst());

        assertEquals(Set.of(1L, 2L, 3L, 4L), ids(merge.inputs()));
        assertEquals(6, merge.targetLevel());
        assertTrue(merge.dropTombstones());
    }

    @Test
    void levelZeroCompactionTakesEveryLevelZeroTableAndTheOverlappingBaseTables() {
        var levels = levels(
            List.of(table(6, "f", "f", 10), table(5, "e", "e", 10), table(4, "d", "d", 10), table(3, "c", "c", 10), table(2, "b", "b", 10)),
            List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of(table(10, "a", "c", 50), table(11, "x", "z", 50))
        );

        CompactionPlanner.Merge merge = assertInstanceOf(CompactionPlanner.Merge.class, planner.plan(levels, Set.of()).getFirst());

        assertEquals(Set.of(2L, 3L, 4L, 5L, 6L, 10L), ids(merge.inputs()));
        assertEquals(6, merge.targetLevel());
    }

    @Test
    void levelZeroWaitsWhileAnyLevelZeroTableIsBusy() {
        var levels = levels(List.of(table(5, "e", "e", 10), table(4, "d", "d", 10), table(3, "c", "c", 10), table(2, "b", "b", 10), table(1, "a", "a", 10)));

        assertTrue(planner.plan(levels, Set.of(1L)).isEmpty());
    }

    @Test
    void baseLevelRisesAsTheBottomLevelGrows() {
        assertEquals(6, planner.baseLevel(levels(List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(table(1, "a", "z", 90)))));
        assertEquals(5, planner.baseLevel(levels(List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(table(1, "a", "z", 900)))));
        assertEquals(4, planner.baseLevel(levels(List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(table(1, "a", "z", 5_000)))));
        assertEquals(1, planner.baseLevel(levels(List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(table(1, "a", "z", 50_000_000_000L)))));
    }

    @Test
    void baseLevelNeverSitsBelowTheFirstNonEmptyLevel() {
        var levels = levels(List.of(), List.of(), List.of(), List.of(table(1, "a", "c", 10)), List.of(), List.of(), List.of(table(2, "a", "z", 200)));

        assertEquals(3, planner.baseLevel(levels));
    }

    @Test
    void levelTargetsGrowByTheMultiplierAndNeverDropBelowTheBase() {
        var levels = levels(List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(table(1, "a", "z", 1_000_000)));

        assertEquals(100_000, planner.targetBytes(levels, 5));
        assertEquals(10_000, planner.targetBytes(levels, 4));
        assertEquals(BASE, planner.targetBytes(levels, 1));
    }

    @Test
    void oversizedLevelPicksTheTableWithTheLeastOverlapRatio() {
        var levels = levels(
            List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of(table(1, "a", "c", 300), table(2, "d", "f", 300)),
            List.of(table(3, "a", "c", 600), table(4, "d", "e", 100))
        );

        CompactionPlanner.Merge merge = assertInstanceOf(CompactionPlanner.Merge.class, planner.plan(levels, Set.of()).getFirst());

        assertEquals(Set.of(2L, 4L), ids(merge.inputs()));
        assertEquals(5, merge.sourceLevel());
    }

    @Test
    void tableWithNoOverlapBelowIsMovedWithoutRewriting() {
        var levels = levels(
            List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of(table(1, "a", "c", 300)),
            List.of(table(2, "x", "z", 600))
        );

        CompactionPlanner.Move move = assertInstanceOf(CompactionPlanner.Move.class, planner.plan(levels, Set.of()).getFirst());

        assertEquals(1, move.table().id());
        assertEquals(6, move.targetLevel());
    }

    @Test
    void tablesWithTombstonesAreRewrittenRatherThanMovedIntoTheBottom() {
        var levels = levels(
            List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of(new TableMetadata(1, Slice.utf8("a"), Slice.utf8("c"), 300, 3)),
            List.of(table(2, "x", "z", 600))
        );

        CompactionPlanner.Merge merge = assertInstanceOf(CompactionPlanner.Merge.class, planner.plan(levels, Set.of()).getFirst());

        assertEquals(Set.of(1L), ids(merge.inputs()));
        assertTrue(merge.dropTombstones());
    }

    @Test
    void tombstonesAreKeptWhenDeeperLevelsOverlap() {
        var levels = levels(
            List.of(), List.of(), List.of(), List.of(),
            List.of(table(1, "d", "f", 500)),
            List.of(table(2, "d", "f", 50)),
            List.of(table(3, "c", "e", 2_000), table(4, "f", "g", 2_000), table(5, "x", "z", 2_000))
        );

        CompactionPlanner.Merge merge = assertInstanceOf(CompactionPlanner.Merge.class, planner.plan(levels, Set.of()).getFirst());

        assertEquals(Set.of(1L, 2L), ids(merge.inputs()));
        assertEquals(Set.of(3L, 4L), ids(merge.grandparents()));
        assertFalse(merge.dropTombstones());
    }

    @Test
    void doesNotCompactOutOfTheBottomLevel() {
        var levels = levels(List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(table(1, "a", "m", 800), table(2, "n", "z", 800)));

        assertTrue(planner.plan(levels, Set.of()).isEmpty());
    }

    @Test
    void skipsSeedsWhoseOverlapIsBusy() {
        var levels = levels(
            List.of(), List.of(), List.of(), List.of(), List.of(),
            List.of(table(1, "a", "c", 300), table(2, "d", "f", 300)),
            List.of(table(3, "a", "c", 10), table(4, "d", "f", 600))
        );

        CompactionPlanner.Task task = planner.plan(levels, Set.of(3L)).getFirst();

        assertEquals(Set.of(2L, 4L), ids(task.inputs()));
    }

    @Test
    void ordersTasksByScore() {
        var levels = levels(
            List.of(table(8, "m", "m", 10), table(7, "n", "n", 10), table(6, "o", "o", 10), table(5, "p", "p", 10)),
            List.of(), List.of(), List.of(), List.of(),
            List.of(table(4, "a", "f", 500), table(3, "g", "l", 500)),
            List.of(table(9, "a", "z", 2_000))
        );

        List<CompactionPlanner.Task> tasks = planner.plan(levels, Set.of());

        assertEquals(2, tasks.size());
        assertEquals(5, tasks.get(0).sourceLevel());
        assertEquals(0, tasks.get(1).sourceLevel());
    }

    @SafeVarargs
    private static List<List<TableMetadata>> levels(List<TableMetadata>... given) {
        List<List<TableMetadata>> levels = new ArrayList<>(List.of(given));
        while (levels.size() < Version.LEVELS) {
            levels.add(List.of());
        }
        return levels;
    }

    private static TableMetadata table(long id, String smallest, String largest, long size) {
        return new TableMetadata(id, Slice.utf8(smallest), Slice.utf8(largest), size, 0);
    }

    private static Set<Long> ids(List<TableMetadata> tables) {
        return tables.stream().map(TableMetadata::id).collect(Collectors.toSet());
    }
}
