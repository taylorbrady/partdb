package io.partdb.storage;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

class MergingIteratorTest {

    private static Slice key(String s) {
        return Slice.of(s);
    }

    private static Slice value(String s) {
        return Slice.of(s);
    }

    private static StoredEntry put(String key, String value, long revision) {
        return new StoredEntry.Value(key(key), value(value), revision);
    }

    private static StoredEntry tombstone(String key, long revision) {
        return new StoredEntry.Tombstone(key(key), revision);
    }

    private static List<StoredEntry> drain(Iterator<StoredEntry> iterator) {
        List<StoredEntry> result = new ArrayList<>();
        iterator.forEachRemaining(result::add);
        return result;
    }

    private static List<String> keys(List<StoredEntry> mutations) {
        return mutations.stream()
            .map(m -> new String(m.key().toByteArray()))
            .toList();
    }

    @Nested
    class EmptyInputs {

        @Test
        void emptyIteratorList() {
            MergingIterator iterator = new MergingIterator(List.of());

            assertFalse(iterator.hasNext());
        }

        @Test
        void singleEmptyIterator() {
            MergingIterator iterator = new MergingIterator(List.of(
                Collections.emptyIterator()
            ));

            assertFalse(iterator.hasNext());
        }

        @Test
        void multipleEmptyIterators() {
            MergingIterator iterator = new MergingIterator(List.of(
                Collections.emptyIterator(),
                Collections.emptyIterator(),
                Collections.emptyIterator()
            ));

            assertFalse(iterator.hasNext());
        }

        @Test
        void someEmptyIterators() {
            MergingIterator iterator = new MergingIterator(List.of(
                Collections.emptyIterator(),
                List.of(put("a", "v1", 1)).iterator(),
                Collections.emptyIterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a"), keys(result));
        }
    }

    @Nested
    class SingleIterator {

        @Test
        void passesThrough() {
            List<StoredEntry> input = List.of(
                put("a", "v1", 1),
                put("b", "v2", 2),
                put("c", "v3", 3)
            );

            MergingIterator iterator = new MergingIterator(List.of(input.iterator()));
            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a", "b", "c"), keys(result));
        }

        @Test
        void singleElement() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(key("a"), result.getFirst().key());
        }
    }

    @Nested
    class MergeOrdering {

        @Test
        void nonOverlappingKeys() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1), put("c", "v3", 3)).iterator(),
                List.of(put("b", "v2", 2), put("d", "v4", 4)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a", "b", "c", "d"), keys(result));
        }

        @Test
        void interleavedKeys() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1), put("c", "v3", 3), put("e", "v5", 5)).iterator(),
                List.of(put("b", "v2", 2), put("d", "v4", 4), put("f", "v6", 6)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a", "b", "c", "d", "e", "f"), keys(result));
        }

        @Test
        void threeIterators() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1), put("d", "v4", 4)).iterator(),
                List.of(put("b", "v2", 2), put("e", "v5", 5)).iterator(),
                List.of(put("c", "v3", 3), put("f", "v6", 6)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a", "b", "c", "d", "e", "f"), keys(result));
        }

        @Test
        void outputIsSorted() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("z", "v1", 1)).iterator(),
                List.of(put("a", "v2", 2)).iterator(),
                List.of(put("m", "v3", 3)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a", "m", "z"), keys(result));
        }
    }

    @Nested
    class DuplicateKeyResolution {

        @Test
        void higherRevisionWins() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "old", 1)).iterator(),
                List.of(put("a", "new", 2)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(2, result.getFirst().revision());
            assertEquals(value("new"), ((StoredEntry.Value) result.getFirst()).value());
        }

        @Test
        void higherRevisionWinsReverseOrder() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "new", 2)).iterator(),
                List.of(put("a", "old", 1)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(2, result.getFirst().revision());
        }

        @Test
        void sameRevisionLowerIndexWins() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "first", 1)).iterator(),
                List.of(put("a", "second", 1)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(value("first"), ((StoredEntry.Value) result.getFirst()).value());
        }

        @Test
        void multipleDuplicatesHighestRevisionWins() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1)).iterator(),
                List.of(put("a", "v2", 3)).iterator(),
                List.of(put("a", "v3", 2)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(3, result.getFirst().revision());
            assertEquals(value("v2"), ((StoredEntry.Value) result.getFirst()).value());
        }

        @Test
        void duplicatesInterleavedWithUnique() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "old", 1), put("c", "c1", 3)).iterator(),
                List.of(put("a", "new", 2), put("b", "b1", 4)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(List.of("a", "b", "c"), keys(result));
            assertEquals(2, result.getFirst().revision());
        }

        @Test
        void consecutiveDuplicatesInSameIterator() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(
                    put("a", "v1", 3),
                    put("a", "v2", 2),
                    put("a", "v3", 1)
                ).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(3, result.getFirst().revision());
        }
    }

    @Nested
    class MutationTypes {

        @Test
        void tombstoneIsPreserved() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(tombstone("a", 1)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertInstanceOf(StoredEntry.Tombstone.class, result.getFirst());
        }

        @Test
        void tombstoneOverwritesPut() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1)).iterator(),
                List.of(tombstone("a", 2)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertInstanceOf(StoredEntry.Tombstone.class, result.getFirst());
        }

        @Test
        void putOverwritesTombstone() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(tombstone("a", 1)).iterator(),
                List.of(put("a", "v1", 2)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertInstanceOf(StoredEntry.Value.class, result.getFirst());
        }

        @Test
        void mixedTypes() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1), tombstone("c", 3)).iterator(),
                List.of(tombstone("b", 2), put("d", "v4", 4)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(4, result.size());
            assertInstanceOf(StoredEntry.Value.class, result.get(0));
            assertInstanceOf(StoredEntry.Tombstone.class, result.get(1));
            assertInstanceOf(StoredEntry.Tombstone.class, result.get(2));
            assertInstanceOf(StoredEntry.Value.class, result.get(3));
        }
    }

    @Nested
    class IteratorContract {

        @Test
        void nextOnExhaustedThrows() {
            MergingIterator iterator = new MergingIterator(List.of());

            assertThrows(NoSuchElementException.class, iterator::next);
        }

        @Test
        void nextAfterDrainThrows() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1)).iterator()
            ));

            iterator.next();

            assertThrows(NoSuchElementException.class, iterator::next);
        }

        @Test
        void hasNextIsIdempotent() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1)).iterator()
            ));

            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());
            assertTrue(iterator.hasNext());

            iterator.next();

            assertFalse(iterator.hasNext());
            assertFalse(iterator.hasNext());
        }

        @Test
        void nextWithoutHasNext() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1), put("b", "v2", 2)).iterator()
            ));

            assertEquals(key("a"), iterator.next().key());
            assertEquals(key("b"), iterator.next().key());
            assertFalse(iterator.hasNext());
        }
    }

    @Nested
    class EdgeCases {

        @Test
        void largeRevisionNumbers() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "min", Long.MIN_VALUE)).iterator(),
                List.of(put("a", "max", Long.MAX_VALUE)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(Long.MAX_VALUE, result.getFirst().revision());
        }

        @Test
        void emptyKeys() {
            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("", "empty", 1)).iterator(),
                List.of(put("a", "v1", 2)).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(2, result.size());
            assertEquals(key(""), result.getFirst().key());
        }

        @Test
        void manyIterators() {
            List<Iterator<StoredEntry>> iterators = new ArrayList<>();
            for (int i = 0; i < 100; i++) {
                iterators.add(List.of(put("key" + String.format("%03d", i), "v" + i, i)).iterator());
            }

            MergingIterator iterator = new MergingIterator(iterators);
            List<StoredEntry> result = drain(iterator);

            assertEquals(100, result.size());
            for (int i = 0; i < 100; i++) {
                assertEquals(key("key" + String.format("%03d", i)), result.get(i).key());
            }
        }

        @Test
        void allSameKey() {
            List<Iterator<StoredEntry>> iterators = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                iterators.add(List.of(put("same", "v" + i, i)).iterator());
            }

            MergingIterator iterator = new MergingIterator(iterators);
            List<StoredEntry> result = drain(iterator);

            assertEquals(1, result.size());
            assertEquals(9, result.getFirst().revision());
        }

        @Test
        void binaryKeyOrdering() {
            StoredEntry highByte = new StoredEntry.Value(
                Slice.of(new byte[]{(byte) 0xFF}),
                value("high"),
                2
            );
            StoredEntry lowByte = new StoredEntry.Value(
                Slice.of(new byte[]{(byte) 0x00}),
                value("low"),
                3
            );

            MergingIterator iterator = new MergingIterator(List.of(
                List.of(put("a", "v1", 1)).iterator(),
                List.of(highByte).iterator(),
                List.of(lowByte).iterator()
            ));

            List<StoredEntry> result = drain(iterator);

            assertEquals(3, result.size());
            assertArrayEquals(new byte[]{0x00}, result.get(0).key().toByteArray());
            assertEquals(key("a"), result.get(1).key());
            assertArrayEquals(new byte[]{(byte) 0xFF}, result.get(2).key().toByteArray());
        }
    }
}
