package io.partdb.raft;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UnstableLogTest {

    private static RaftLogEntry entry(long index, long term) {
        return new RaftLogEntry.NoOp(index, term);
    }

    @Test
    void appendsAtOffset() {
        var unstable = new UnstableLog(1);

        unstable.append(entry(1, 1));
        unstable.append(entry(2, 1));

        assertEquals(List.of(entry(1, 1), entry(2, 1)), unstable.slice(1, 3));
        assertEquals(OptionalLong.of(2), unstable.lastIndex());
    }

    @Test
    void truncatesConflictingSuffix() {
        var unstable = new UnstableLog(1);
        unstable.append(entry(1, 1));
        unstable.append(entry(2, 1));
        unstable.append(entry(3, 1));

        unstable.append(entry(2, 2));

        assertEquals(List.of(entry(1, 1), entry(2, 2)), unstable.slice(1, 4));
    }

    @Test
    void takesOverStableSuffixWhenAppendingBelowOffset() {
        var unstable = new UnstableLog(5);

        unstable.append(entry(3, 2));

        assertEquals(3, unstable.offset());
        assertEquals(List.of(entry(3, 2)), unstable.slice(1, 10));
    }

    @Test
    void rejectsGaps() {
        var unstable = new UnstableLog(1);

        assertThrows(IllegalStateException.class, () -> unstable.append(entry(3, 1)));
    }

    @Test
    void stableToDropsPersistedPrefix() {
        var unstable = new UnstableLog(1);
        unstable.append(entry(1, 1));
        unstable.append(entry(2, 1));

        unstable.stableTo(1, 1);

        assertEquals(2, unstable.offset());
        assertEquals(List.of(entry(2, 1)), unstable.slice(1, 3));
    }

    @Test
    void stableToIgnoresMismatchedTerm() {
        var unstable = new UnstableLog(1);
        unstable.append(entry(1, 2));

        unstable.stableTo(1, 1);

        assertEquals(1, unstable.offset());
        assertTrue(unstable.hasEntries());
    }

    @Test
    void restoreReplacesEntriesWithSnapshot() {
        var unstable = new UnstableLog(1);
        unstable.append(entry(1, 1));
        var snapshot = new SnapshotMetadata(5, 2, RaftMembership.voters("n1"));

        unstable.restore(snapshot);

        assertTrue(!unstable.hasEntries());
        assertEquals(6, unstable.offset());
        assertEquals(OptionalLong.of(5), unstable.lastIndex());
        assertEquals(OptionalLong.of(2), unstable.term(5));
        unstable.stableTo(5, 2);
        assertEquals(Optional.empty(), unstable.snapshot());
    }

    @Test
    void takesEachEntryAndSnapshotOnce() {
        var unstable = new UnstableLog(1);
        unstable.append(entry(1, 1));
        assertEquals(List.of(entry(1, 1)), unstable.takeEntries());
        assertTrue(unstable.takeEntries().isEmpty());

        unstable.append(entry(2, 1));
        assertEquals(List.of(entry(2, 1)), unstable.takeEntries());

        unstable.append(entry(2, 2));
        assertEquals(List.of(entry(2, 2)), unstable.takeEntries());

        var snapshot = new SnapshotMetadata(5, 2, RaftMembership.voters("n1"));
        unstable.restore(snapshot);
        assertEquals(Optional.of(snapshot), unstable.takeSnapshot());
        assertEquals(Optional.empty(), unstable.takeSnapshot());
    }

    @Test
    void termIsEmptyOutsideUnstableRange() {
        var unstable = new UnstableLog(3);
        unstable.append(entry(3, 1));

        assertEquals(OptionalLong.empty(), unstable.term(2));
        assertEquals(OptionalLong.empty(), unstable.term(4));
        assertEquals(OptionalLong.of(1), unstable.term(3));
    }
}
