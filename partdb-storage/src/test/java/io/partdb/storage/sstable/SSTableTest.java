package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

class SSTableTest {

    @TempDir
    Path tempDir;

    @Test
    void writeAndReadSingleEntry() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        Entry entry = Entry.put(key, value, 1000L);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(entry);
        }

        assertThat(Files.exists(sstablePath)).isTrue();

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<Entry> result = reader.get(key);

            assertThat(result).isPresent();
            assertThat(result.get().key()).isEqualTo(key);
            assertThat(result.get().value()).isEqualTo(value);
            assertThat(result.get().timestamp()).isEqualTo(1000L);
            assertThat(result.get().tombstone()).isFalse();
        }
    }

    @Test
    void writeAndReadMultipleEntries() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        List<Entry> entries = List.of(
            Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L),
            Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L),
            Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L)
        );

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (Entry entry : entries) {
                writer.append(entry);
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (Entry entry : entries) {
                Optional<Entry> result = reader.get(entry.key());
                assertThat(result).isPresent();
                assertThat(result.get().value()).isEqualTo(entry.value());
            }
        }
    }

    @Test
    void getNonExistentKeyReturnsEmpty() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<Entry> result = reader.get(ByteArray.of((byte) 99));
            assertThat(result).isEmpty();
        }
    }

    @Test
    void writeAndReadTombstone() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        ByteArray key = ByteArray.of((byte) 5);
        Entry deleteEntry = Entry.delete(key, 1000L);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(deleteEntry);
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<Entry> result = reader.get(key);

            assertThat(result).isPresent();
            assertThat(result.get().tombstone()).isTrue();
            assertThat(result.get().value()).isNull();
        }
    }

    @Test
    void writeAndReadEntryWithTTL() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);
        Entry entry = Entry.putWithTTL(key, value, 1000L, 5000L);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(entry);
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<Entry> result = reader.get(key);

            assertThat(result).isPresent();
            assertThat(result.get().expiresAtMillis()).isEqualTo(6000L);
        }
    }

    @Test
    void writerRejectsUnsortedEntries() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));

            assertThatThrownBy(() ->
                writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L))
            )
            .isInstanceOf(SSTableException.class)
            .hasMessageContaining("ascending order");
        }
    }

    @Test
    void writerRejectsDuplicateKeys() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        ByteArray key = ByteArray.of((byte) 1);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(key, ByteArray.of((byte) 10), 100L));

            assertThatThrownBy(() ->
                writer.append(Entry.put(key, ByteArray.of((byte) 20), 200L))
            )
            .isInstanceOf(SSTableException.class)
            .hasMessageContaining("ascending order");
        }
    }

    @Test
    void scanEntireRange() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
            writer.append(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
            writer.append(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<Entry> it = reader.scan(null, null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(3);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(2).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanWithStartKeyOnly() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
            writer.append(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
            writer.append(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<Entry> it = reader.scan(ByteArray.of((byte) 2), null);
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void scanWithEndKeyOnly() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
            writer.append(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
            writer.append(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<Entry> it = reader.scan(null, ByteArray.of((byte) 3));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 1));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 2));
        }
    }

    @Test
    void scanWithBothStartAndEnd() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
            writer.append(Entry.put(ByteArray.of((byte) 2), ByteArray.of((byte) 20), 200L));
            writer.append(Entry.put(ByteArray.of((byte) 3), ByteArray.of((byte) 30), 300L));
            writer.append(Entry.put(ByteArray.of((byte) 4), ByteArray.of((byte) 40), 400L));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<Entry> it = reader.scan(ByteArray.of((byte) 2), ByteArray.of((byte) 4));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).key()).isEqualTo(ByteArray.of((byte) 2));
            assertThat(entries.get(1).key()).isEqualTo(ByteArray.of((byte) 3));
        }
    }

    @Test
    void multipleBlocksWithSmallBlockSize() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = new SSTableConfig(256, SSTableConfig.DEFAULT_BLOOM_FILTER_FPR);

        byte[] largeValue = new byte[100];
        List<Entry> entries = new ArrayList<>();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                Entry entry = Entry.put(key, value, i * 100L);
                entries.add(entry);
                writer.append(entry);
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (Entry entry : entries) {
                Optional<Entry> result = reader.get(entry.key());
                assertThat(result).isPresent();
                assertThat(result.get().key()).isEqualTo(entry.key());
            }

            Iterator<Entry> it = reader.scan(null, null);
            List<Entry> scanned = collectEntries(it);
            assertThat(scanned).hasSize(20);
        }
    }

    @Test
    void scanEmptyRange() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(Entry.put(ByteArray.of((byte) 1), ByteArray.of((byte) 10), 100L));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<Entry> it = reader.scan(ByteArray.of((byte) 10), ByteArray.of((byte) 20));
            List<Entry> entries = collectEntries(it);

            assertThat(entries).isEmpty();
        }
    }

    @Test
    void bloomFilterFiltersMissingKeys() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.of((byte) (i * 2));
                writer.append(Entry.put(key, value, i * 100L));
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (int i = 100; i < 200; i++) {
                ByteArray missingKey = ByteArray.of((byte) i);
                Optional<Entry> result = reader.get(missingKey);
                assertThat(result).isEmpty();
            }
        }
    }

    @Test
    void bloomFilterDoesNotCauseFalseNegatives() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        List<Entry> entries = new ArrayList<>();
        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.of((byte) (i * 2));
                Entry entry = Entry.put(key, value, i * 100L);
                entries.add(entry);
                writer.append(entry);
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (Entry entry : entries) {
                Optional<Entry> result = reader.get(entry.key());
                assertThat(result).isPresent();
                assertThat(result.get().key()).isEqualTo(entry.key());
                assertThat(result.get().value()).isEqualTo(entry.value());
            }
        }
    }

    @Test
    void bloomFilterWithCustomFalsePositiveRate() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = new SSTableConfig(SSTableConfig.DEFAULT_BLOCK_SIZE, 0.001);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.of((byte) (i * 2));
                writer.append(Entry.put(key, value, i * 100L));
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.of((byte) i);
                Optional<Entry> result = reader.get(key);
                assertThat(result).isPresent();
            }
        }
    }

    private List<Entry> collectEntries(Iterator<Entry> iterator) {
        List<Entry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }
}
