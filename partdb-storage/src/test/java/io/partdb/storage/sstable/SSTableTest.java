package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class SSTableTest {

    @TempDir
    Path tempDir;

    private static ByteArray key(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray value(int i) {
        return ByteArray.of((byte) i);
    }

    private static Entry.Put entry(int key, int value) {
        return new Entry.Put(key(key), Timestamp.of(key, 0), value(value));
    }

    private static ByteArray largeValue(int size) {
        return ByteArray.copyOf(new byte[size]);
    }

    private List<Entry> collectEntries(Iterator<Entry> iterator) {
        List<Entry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }

    @Nested
    class ReadWrite {

        @Test
        void singleEntry() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 2));
            }

            assertTrue(Files.exists(path));

            try (SSTable table = SSTable.open(path)) {
                Optional<Entry> result = table.get(key(1), Timestamp.MAX);

                assertTrue(result.isPresent());
                assertEquals(key(1), result.get().key());
                assertInstanceOf(Entry.Put.class, result.get());
                assertEquals(value(2), ((Entry.Put) result.get()).value());
            }
        }

        @Test
        void multipleEntries() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            List<Entry.Put> entries = List.of(entry(1, 10), entry(2, 20), entry(3, 30));

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                for (Entry e : entries) {
                    writer.add(e);
                }
            }

            try (SSTable table = SSTable.open(path)) {
                for (Entry.Put e : entries) {
                    Optional<Entry> result = table.get(e.key(), Timestamp.MAX);
                    assertTrue(result.isPresent());
                    assertEquals(e.value(), ((Entry.Put) result.get()).value());
                }
            }
        }

        @Test
        void nonExistentKeyReturnsEmpty() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));
            }

            try (SSTable table = SSTable.open(path)) {
                Optional<Entry> result = table.get(key(99), Timestamp.MAX);
                assertTrue(result.isEmpty());
            }
        }

        @Test
        void tombstone() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(new Entry.Tombstone(key(5), Timestamp.of(5, 0)));
            }

            try (SSTable table = SSTable.open(path)) {
                Optional<Entry> result = table.get(key(5), Timestamp.MAX);

                assertTrue(result.isPresent());
                assertInstanceOf(Entry.Tombstone.class, result.get());
            }
        }

        @Test
        void multipleBlocksWithSmallBlockSize() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = new SSTableConfig(256, SSTableConfig.DEFAULT_BLOOM_FILTER_FPR);

            List<Entry> entries = new ArrayList<>();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                for (int i = 0; i < 20; i++) {
                    Entry e = new Entry.Put(key(i), Timestamp.of(i, 0), largeValue(100));
                    entries.add(e);
                    writer.add(e);
                }
            }

            try (SSTable table = SSTable.open(path)) {
                for (Entry e : entries) {
                    Optional<Entry> result = table.get(e.key(), Timestamp.MAX);
                    assertTrue(result.isPresent());
                    assertEquals(e.key(), result.get().key());
                }

                Iterator<Entry> it = table.scan().asOf(Timestamp.MAX).iterator();
                List<Entry> scanned = collectEntries(it);
                assertEquals(20, scanned.size());
            }
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsUnsortedEntries() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(2, 20));

                SSTableException ex = assertThrows(SSTableException.class, () -> writer.add(entry(1, 10)));
                assertTrue(ex.getMessage().contains("ascending order"));
            }
        }

        @Test
        void rejectsDuplicateKeys() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));

                SSTableException ex = assertThrows(SSTableException.class, () -> writer.add(entry(1, 20)));
                assertTrue(ex.getMessage().contains("ascending order"));
            }
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));
                writer.add(entry(2, 20));
                writer.add(entry(3, 30));
            }

            try (SSTable table = SSTable.open(path)) {
                Iterator<Entry> it = table.scan().asOf(Timestamp.MAX).iterator();
                List<Entry> entries = collectEntries(it);

                assertEquals(3, entries.size());
                assertEquals(key(1), entries.get(0).key());
                assertEquals(key(2), entries.get(1).key());
                assertEquals(key(3), entries.get(2).key());
            }
        }

        @Test
        void withStartKeyOnly() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));
                writer.add(entry(2, 20));
                writer.add(entry(3, 30));
            }

            try (SSTable table = SSTable.open(path)) {
                Iterator<Entry> it = table.scan().from(key(2)).asOf(Timestamp.MAX).iterator();
                List<Entry> entries = collectEntries(it);

                assertEquals(2, entries.size());
                assertEquals(key(2), entries.get(0).key());
                assertEquals(key(3), entries.get(1).key());
            }
        }

        @Test
        void withEndKeyOnly() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));
                writer.add(entry(2, 20));
                writer.add(entry(3, 30));
            }

            try (SSTable table = SSTable.open(path)) {
                Iterator<Entry> it = table.scan().until(key(3)).asOf(Timestamp.MAX).iterator();
                List<Entry> entries = collectEntries(it);

                assertEquals(2, entries.size());
                assertEquals(key(1), entries.get(0).key());
                assertEquals(key(2), entries.get(1).key());
            }
        }

        @Test
        void withBothBounds() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));
                writer.add(entry(2, 20));
                writer.add(entry(3, 30));
                writer.add(entry(4, 40));
            }

            try (SSTable table = SSTable.open(path)) {
                Iterator<Entry> it = table.scan().from(key(2)).until(key(4)).asOf(Timestamp.MAX).iterator();
                List<Entry> entries = collectEntries(it);

                assertEquals(2, entries.size());
                assertEquals(key(2), entries.get(0).key());
                assertEquals(key(3), entries.get(1).key());
            }
        }

        @Test
        void emptyRange() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                writer.add(entry(1, 10));
            }

            try (SSTable table = SSTable.open(path)) {
                Iterator<Entry> it = table.scan().from(key(10)).until(key(20)).asOf(Timestamp.MAX).iterator();
                List<Entry> entries = collectEntries(it);

                assertTrue(entries.isEmpty());
            }
        }
    }

    @Nested
    class BloomFilter {

        @Test
        void filtersMissingKeys() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                for (int i = 0; i < 100; i++) {
                    writer.add(entry(i, i * 2));
                }
            }

            try (SSTable table = SSTable.open(path)) {
                for (int i = 100; i < 200; i++) {
                    Optional<Entry> result = table.get(key(i), Timestamp.MAX);
                    assertTrue(result.isEmpty());
                }
            }
        }

        @Test
        void noFalseNegatives() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.defaults();

            List<Entry.Put> entries = new ArrayList<>();
            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                for (int i = 0; i < 100; i++) {
                    Entry.Put e = entry(i, i * 2);
                    entries.add(e);
                    writer.add(e);
                }
            }

            try (SSTable table = SSTable.open(path)) {
                for (Entry.Put e : entries) {
                    Optional<Entry> result = table.get(e.key(), Timestamp.MAX);
                    assertTrue(result.isPresent());
                    assertEquals(e.key(), result.get().key());
                    assertEquals(e.value(), ((Entry.Put) result.get()).value());
                }
            }
        }

        @Test
        void customFalsePositiveRate() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = new SSTableConfig(SSTableConfig.DEFAULT_BLOCK_SIZE, 0.001);

            try (SSTable.Writer writer = SSTable.Writer.create(path, config)) {
                for (int i = 0; i < 50; i++) {
                    writer.add(entry(i, i * 2));
                }
            }

            try (SSTable table = SSTable.open(path)) {
                for (int i = 0; i < 50; i++) {
                    Optional<Entry> result = table.get(key(i), Timestamp.MAX);
                    assertTrue(result.isPresent());
                }
            }
        }
    }
}
