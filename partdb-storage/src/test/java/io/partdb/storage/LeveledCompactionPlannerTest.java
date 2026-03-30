package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeveledCompactionPlannerTest {

    @Test
    void l0CompactionUsesBoundedOldestBatch() {
        LSMConfig config = LSMConfig.defaults()
            .withL0CompactionTrigger(4)
            .withMaxLevels(4);
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(config);

        Manifest manifest = new Manifest(10, List.of(
            sstable(5, 0, "e", "e", 10),
            sstable(4, 0, "d", "d", 10),
            sstable(3, 0, "c", "c", 10),
            sstable(2, 0, "b", "b", 10),
            sstable(1, 0, "a", "a", 10),
            sstable(10, 1, "a", "z", 10)
        ));

        List<CompactionTask> tasks = planner.selectCompactions(manifest, Set.of());

        assertEquals(1, tasks.size());
        Set<Long> inputIds = ids(tasks.get(0));
        assertEquals(Set.of(1L, 2L, 3L, 4L, 10L), inputIds);
        assertFalse(inputIds.contains(5L));
    }

    @Test
    void doesNotCompactPastConfiguredLastLevel() {
        LSMConfig config = LSMConfig.defaults()
            .withMaxLevels(3)
            .withMaxBytesForLevelBase(100);
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(config);

        Manifest manifest = new Manifest(3, List.of(
            sstable(1, 2, "a", "m", 800),
            sstable(2, 2, "n", "z", 800)
        ));

        List<CompactionTask> tasks = planner.selectCompactions(manifest, Set.of());

        assertTrue(tasks.isEmpty());
    }

    @Test
    void ordersTasksByCompactionScore() {
        LSMConfig config = LSMConfig.defaults()
            .withL0CompactionTrigger(4)
            .withMaxLevels(4)
            .withMaxBytesForLevelBase(100);
        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(config);

        Manifest manifest = new Manifest(8, List.of(
            sstable(8, 0, "m", "m", 10),
            sstable(7, 0, "n", "n", 10),
            sstable(6, 0, "o", "o", 10),
            sstable(5, 0, "p", "p", 10),
            sstable(4, 1, "a", "f", 150),
            sstable(3, 1, "g", "l", 150)
        ));

        List<CompactionTask> tasks = planner.selectCompactions(manifest, Set.of());

        assertEquals(2, tasks.size());
        assertEquals(2, tasks.get(0).targetLevel());
        assertEquals(1, tasks.get(1).targetLevel());
    }

    private static Set<Long> ids(CompactionTask task) {
        return task.inputs().stream()
            .map(SSTableDescriptor::id)
            .collect(java.util.stream.Collectors.toSet());
    }

    private static SSTableDescriptor sstable(long id, int level, String smallest, String largest, long fileSize) {
        return new SSTableDescriptor(
            id,
            level,
            slice(smallest),
            slice(largest),
            id,
            id,
            fileSize,
            1
        );
    }

    private static Slice slice(String value) {
        return Slice.of(value.getBytes(StandardCharsets.UTF_8));
    }
}
