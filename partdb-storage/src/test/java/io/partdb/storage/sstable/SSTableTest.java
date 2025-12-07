package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SSTableTest {

    @TempDir
    Path tempDir;

    private static ByteArray key(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray value(int i) {
        return ByteArray.of((byte) i);
    }

    private static Entry.Data entry(int key, int value) {
        return new Entry.Data(key(key), value(value));
    }

    private static ByteArray largeValue(int size) {
        return ByteArray.wrap(new byte[size]);
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
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 2));
            }

            assertThat(Files.exists(path)).isTrue();

            try (SSTableReader reader = SSTableReader.open(path)) {
                Optional<Entry> result = reader.get(key(1));

                assertThat(result).isPresent();
                assertThat(result.get().key()).isEqualTo(key(1));
                assertThat(result.get()).isInstanceOf(Entry.Data.class);
                assertThat(((Entry.Data) result.get()).value()).isEqualTo(value(2));
            }
        }

        @Test
        void multipleEntries() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            List<Entry.Data> entries = List.of(entry(1, 10), entry(2, 20), entry(3, 30));

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                for (Entry e : entries) {
                    writer.append(e);
                }
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                for (Entry.Data e : entries) {
                    Optional<Entry> result = reader.get(e.key());
                    assertThat(result).isPresent();
                    assertThat(((Entry.Data) result.get()).value()).isEqualTo(e.value());
                }
            }
        }

        @Test
        void nonExistentKeyReturnsEmpty() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Optional<Entry> result = reader.get(key(99));
                assertThat(result).isEmpty();
            }
        }

        @Test
        void tombstone() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(new Entry.Tombstone(key(5)));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Optional<Entry> result = reader.get(key(5));

                assertThat(result).isPresent();
                assertThat(result.get()).isInstanceOf(Entry.Tombstone.class);
            }
        }

        @Test
        void multipleBlocksWithSmallBlockSize() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = new SSTableConfig(256, SSTableConfig.DEFAULT_BLOOM_FILTER_FPR);

            List<Entry> entries = new ArrayList<>();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                for (int i = 0; i < 20; i++) {
                    Entry e = new Entry.Data(key(i), largeValue(100));
                    entries.add(e);
                    writer.append(e);
                }
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                for (Entry e : entries) {
                    Optional<Entry> result = reader.get(e.key());
                    assertThat(result).isPresent();
                    assertThat(result.get().key()).isEqualTo(e.key());
                }

                Iterator<Entry> it = reader.scan(null, null);
                List<Entry> scanned = collectEntries(it);
                assertThat(scanned).hasSize(20);
            }
        }
    }

    @Nested
    class Validation {

        @Test
        void rejectsUnsortedEntries() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(2, 20));

                assertThatThrownBy(() -> writer.append(entry(1, 10)))
                    .isInstanceOf(SSTableException.class)
                    .hasMessageContaining("ascending order");
            }
        }

        @Test
        void rejectsDuplicateKeys() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));

                assertThatThrownBy(() -> writer.append(entry(1, 20)))
                    .isInstanceOf(SSTableException.class)
                    .hasMessageContaining("ascending order");
            }
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));
                writer.append(entry(2, 20));
                writer.append(entry(3, 30));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Iterator<Entry> it = reader.scan(null, null);
                List<Entry> entries = collectEntries(it);

                assertThat(entries).hasSize(3);
                assertThat(entries.get(0).key()).isEqualTo(key(1));
                assertThat(entries.get(1).key()).isEqualTo(key(2));
                assertThat(entries.get(2).key()).isEqualTo(key(3));
            }
        }

        @Test
        void withStartKeyOnly() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));
                writer.append(entry(2, 20));
                writer.append(entry(3, 30));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Iterator<Entry> it = reader.scan(key(2), null);
                List<Entry> entries = collectEntries(it);

                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).key()).isEqualTo(key(2));
                assertThat(entries.get(1).key()).isEqualTo(key(3));
            }
        }

        @Test
        void withEndKeyOnly() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));
                writer.append(entry(2, 20));
                writer.append(entry(3, 30));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Iterator<Entry> it = reader.scan(null, key(3));
                List<Entry> entries = collectEntries(it);

                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).key()).isEqualTo(key(1));
                assertThat(entries.get(1).key()).isEqualTo(key(2));
            }
        }

        @Test
        void withBothBounds() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));
                writer.append(entry(2, 20));
                writer.append(entry(3, 30));
                writer.append(entry(4, 40));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Iterator<Entry> it = reader.scan(key(2), key(4));
                List<Entry> entries = collectEntries(it);

                assertThat(entries).hasSize(2);
                assertThat(entries.get(0).key()).isEqualTo(key(2));
                assertThat(entries.get(1).key()).isEqualTo(key(3));
            }
        }

        @Test
        void emptyRange() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                writer.append(entry(1, 10));
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                Iterator<Entry> it = reader.scan(key(10), key(20));
                List<Entry> entries = collectEntries(it);

                assertThat(entries).isEmpty();
            }
        }
    }

    @Nested
    class BloomFilter {

        @Test
        void filtersMissingKeys() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                for (int i = 0; i < 100; i++) {
                    writer.append(entry(i, i * 2));
                }
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                for (int i = 100; i < 200; i++) {
                    Optional<Entry> result = reader.get(key(i));
                    assertThat(result).isEmpty();
                }
            }
        }

        @Test
        void noFalseNegatives() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = SSTableConfig.create();

            List<Entry.Data> entries = new ArrayList<>();
            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                for (int i = 0; i < 100; i++) {
                    Entry.Data e = entry(i, i * 2);
                    entries.add(e);
                    writer.append(e);
                }
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                for (Entry.Data e : entries) {
                    Optional<Entry> result = reader.get(e.key());
                    assertThat(result).isPresent();
                    assertThat(result.get().key()).isEqualTo(e.key());
                    assertThat(((Entry.Data) result.get()).value()).isEqualTo(e.value());
                }
            }
        }

        @Test
        void customFalsePositiveRate() {
            Path path = tempDir.resolve("test.sst");
            SSTableConfig config = new SSTableConfig(SSTableConfig.DEFAULT_BLOCK_SIZE, 0.001);

            try (SSTableWriter writer = SSTableWriter.create(path, config)) {
                for (int i = 0; i < 50; i++) {
                    writer.append(entry(i, i * 2));
                }
            }

            try (SSTableReader reader = SSTableReader.open(path)) {
                for (int i = 0; i < 50; i++) {
                    Optional<Entry> result = reader.get(key(i));
                    assertThat(result).isPresent();
                }
            }
        }
    }
}
