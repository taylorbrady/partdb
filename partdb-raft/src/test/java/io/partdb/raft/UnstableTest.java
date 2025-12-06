package io.partdb.raft;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class UnstableTest {

    private static final Membership MEMBERSHIP = Membership.ofVoters("n1", "n2", "n3");

    private LogEntry entry(long index, long term) {
        return new LogEntry.Data(index, term, new byte[0]);
    }

    @Nested
    class Append {
        @Test
        void addsEntryToEmpty() {
            var unstable = new Unstable(1);

            unstable.append(entry(1, 1));

            assertEquals(1, unstable.entries().size());
            assertEquals(1, unstable.lastIndex());
            assertEquals(1, unstable.lastTerm());
        }

        @Test
        void addsEntriesSequentially() {
            var unstable = new Unstable(1);

            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));
            unstable.append(entry(3, 1));

            assertEquals(3, unstable.entries().size());
            assertEquals(3, unstable.lastIndex());
        }

        @Test
        void ignoresEntriesBelowOffset() {
            var unstable = new Unstable(5);

            unstable.append(entry(3, 1));
            unstable.append(entry(4, 1));

            assertTrue(unstable.entries().isEmpty());
        }

        @Test
        void truncatesConflictingEntries() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));
            unstable.append(entry(3, 1));

            unstable.append(entry(2, 2));

            assertEquals(2, unstable.entries().size());
            assertEquals(2, unstable.lastIndex());
            assertEquals(2, unstable.lastTerm());
        }

        @Test
        void doesNotTruncateMatchingEntries() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));

            unstable.append(entry(2, 1));

            assertEquals(2, unstable.entries().size());
        }
    }

    @Nested
    class AcceptSnapshot {
        @Test
        void setsSnapshot() {
            var unstable = new Unstable(1);
            var snapshot = new Snapshot(10, 2, MEMBERSHIP, new byte[0]);

            unstable.acceptSnapshot(snapshot);

            assertTrue(unstable.hasSnapshot());
            assertEquals(snapshot, unstable.snapshot());
        }

        @Test
        void clearsEntries() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));

            unstable.acceptSnapshot(new Snapshot(10, 2, MEMBERSHIP, new byte[0]));

            assertFalse(unstable.hasEntries());
        }

        @Test
        void updatesOffset() {
            var unstable = new Unstable(1);

            unstable.acceptSnapshot(new Snapshot(10, 2, MEMBERSHIP, new byte[0]));

            assertEquals(11, unstable.offset());
        }
    }

    @Nested
    class StableTo {
        @Test
        void removesEntriesUpToIndex() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));
            unstable.append(entry(3, 1));

            unstable.stableTo(2, 1);

            assertEquals(1, unstable.entries().size());
            assertEquals(3, unstable.offset());
            assertNull(unstable.get(1));
            assertNull(unstable.get(2));
            assertNotNull(unstable.get(3));
        }

        @Test
        void removesAllEntriesWhenStableToLast() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));

            unstable.stableTo(2, 1);

            assertFalse(unstable.hasEntries());
            assertEquals(3, unstable.offset());
        }

        @Test
        void doesNothingIfIndexBelowOffset() {
            var unstable = new Unstable(5);
            unstable.append(entry(5, 1));

            unstable.stableTo(3, 1);

            assertEquals(1, unstable.entries().size());
            assertEquals(5, unstable.offset());
        }

        @Test
        void doesNothingIfTermMismatch() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));

            unstable.stableTo(1, 2);

            assertEquals(2, unstable.entries().size());
            assertEquals(1, unstable.offset());
        }

        @Test
        void doesNothingIfEntriesEmpty() {
            var unstable = new Unstable(1);

            unstable.stableTo(1, 1);

            assertEquals(1, unstable.offset());
        }

        @Test
        void doesNothingIfIndexBeyondEntries() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));

            unstable.stableTo(5, 1);

            assertEquals(1, unstable.entries().size());
        }
    }

    @Nested
    class SnapshotStabilized {
        @Test
        void clearsSnapshot() {
            var unstable = new Unstable(1);
            unstable.acceptSnapshot(new Snapshot(10, 2, MEMBERSHIP, new byte[0]));
            assertTrue(unstable.hasSnapshot());

            unstable.snapshotStabilized();

            assertFalse(unstable.hasSnapshot());
            assertNull(unstable.snapshot());
        }
    }

    @Nested
    class Get {
        @Test
        void returnsEntryAtIndex() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 2));

            var entry = unstable.get(2);

            assertNotNull(entry);
            assertEquals(2, entry.index());
            assertEquals(2, entry.term());
        }

        @Test
        void returnsNullIfBelowOffset() {
            var unstable = new Unstable(5);
            unstable.append(entry(5, 1));

            assertNull(unstable.get(3));
        }

        @Test
        void returnsNullIfBeyondEntries() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));

            assertNull(unstable.get(5));
        }
    }

    @Nested
    class Term {
        @Test
        void returnsTermFromEntry() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 3));

            assertEquals(3L, unstable.term(1));
        }

        @Test
        void returnsTermFromSnapshot() {
            var unstable = new Unstable(1);
            unstable.acceptSnapshot(new Snapshot(10, 5, MEMBERSHIP, new byte[0]));

            assertEquals(5L, unstable.term(10));
        }

        @Test
        void returnsNullIfNotFound() {
            var unstable = new Unstable(1);

            assertNull(unstable.term(1));
        }
    }

    @Nested
    class Slice {
        @Test
        void returnsEntriesInRange() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));
            unstable.append(entry(2, 1));
            unstable.append(entry(3, 1));

            var slice = unstable.slice(1, 3);

            assertEquals(2, slice.size());
            assertEquals(1, slice.get(0).index());
            assertEquals(2, slice.get(1).index());
        }

        @Test
        void returnsEmptyIfFromAfterEntries() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));

            var slice = unstable.slice(5, 10);

            assertTrue(slice.isEmpty());
        }

        @Test
        void returnsEmptyIfToBeforeOffset() {
            var unstable = new Unstable(5);
            unstable.append(entry(5, 1));

            var slice = unstable.slice(1, 3);

            assertTrue(slice.isEmpty());
        }

        @Test
        void returnsEmptyIfFromEqualsTo() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 1));

            var slice = unstable.slice(1, 1);

            assertTrue(slice.isEmpty());
        }

        @Test
        void clampsRangeToAvailableEntries() {
            var unstable = new Unstable(3);
            unstable.append(entry(3, 1));
            unstable.append(entry(4, 1));
            unstable.append(entry(5, 1));

            var slice = unstable.slice(1, 10);

            assertEquals(3, slice.size());
        }
    }

    @Nested
    class LastIndexAndTerm {
        @Test
        void returnsFromEntriesWhenPresent() {
            var unstable = new Unstable(1);
            unstable.append(entry(1, 2));
            unstable.append(entry(2, 3));

            assertEquals(2, unstable.lastIndex());
            assertEquals(3, unstable.lastTerm());
        }

        @Test
        void returnsFromSnapshotWhenNoEntries() {
            var unstable = new Unstable(1);
            unstable.acceptSnapshot(new Snapshot(10, 5, MEMBERSHIP, new byte[0]));

            assertEquals(10, unstable.lastIndex());
            assertEquals(5, unstable.lastTerm());
        }

        @Test
        void returnsZeroWhenEmpty() {
            var unstable = new Unstable(1);

            assertEquals(0, unstable.lastIndex());
            assertEquals(0, unstable.lastTerm());
        }

        @Test
        void prefersEntriesOverSnapshot() {
            var unstable = new Unstable(1);
            unstable.acceptSnapshot(new Snapshot(10, 5, MEMBERSHIP, new byte[0]));
            unstable.append(entry(11, 6));

            assertEquals(11, unstable.lastIndex());
            assertEquals(6, unstable.lastTerm());
        }
    }

    @Nested
    class HasEntriesAndSnapshot {
        @Test
        void hasEntriesReturnsTrueWhenNotEmpty() {
            var unstable = new Unstable(1);
            assertFalse(unstable.hasEntries());

            unstable.append(entry(1, 1));

            assertTrue(unstable.hasEntries());
        }

        @Test
        void hasSnapshotReturnsTrueWhenSet() {
            var unstable = new Unstable(1);
            assertFalse(unstable.hasSnapshot());

            unstable.acceptSnapshot(new Snapshot(10, 2, MEMBERSHIP, new byte[0]));

            assertTrue(unstable.hasSnapshot());
        }
    }
}
