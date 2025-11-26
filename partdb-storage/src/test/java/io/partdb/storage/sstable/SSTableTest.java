package io.partdb.storage.sstable;

import io.partdb.common.ByteArray;
import io.partdb.storage.StoreEntry;
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
        StoreEntry entry = StoreEntry.of(key, value);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(entry);
        }

        assertThat(Files.exists(sstablePath)).isTrue();

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<StoreEntry> result = reader.get(key);

            assertThat(result).isPresent();
            assertThat(result.get().key()).isEqualTo(key);
            assertThat(result.get().value()).isEqualTo(value);
            assertThat(result.get().tombstone()).isFalse();
        }
    }

    @Test
    void writeAndReadMultipleEntries() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        List<StoreEntry> entries = List.of(
            StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)),
            StoreEntry.of(ByteArray.of((byte) 2), ByteArray.of((byte) 20)),
            StoreEntry.of(ByteArray.of((byte) 3), ByteArray.of((byte) 30))
        );

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (StoreEntry entry : entries) {
                writer.append(entry);
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (StoreEntry entry : entries) {
                Optional<StoreEntry> result = reader.get(entry.key());
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
            writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<StoreEntry> result = reader.get(ByteArray.of((byte) 99));
            assertThat(result).isEmpty();
        }
    }

    @Test
    void writeAndReadTombstone() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        ByteArray key = ByteArray.of((byte) 5);
        StoreEntry deleteEntry = StoreEntry.tombstone(key);

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(deleteEntry);
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Optional<StoreEntry> result = reader.get(key);

            assertThat(result).isPresent();
            assertThat(result.get().tombstone()).isTrue();
            assertThat(result.get().value()).isNull();
        }
    }

    @Test
    void writerRejectsUnsortedEntries() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(StoreEntry.of(ByteArray.of((byte) 2), ByteArray.of((byte) 20)));

            assertThatThrownBy(() ->
                writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)))
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
            writer.append(StoreEntry.of(key, ByteArray.of((byte) 10)));

            assertThatThrownBy(() ->
                writer.append(StoreEntry.of(key, ByteArray.of((byte) 20)))
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
            writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 2), ByteArray.of((byte) 20)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 3), ByteArray.of((byte) 30)));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<StoreEntry> it = reader.scan(null, null);
            List<StoreEntry> entries = collectEntries(it);

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
            writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 2), ByteArray.of((byte) 20)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 3), ByteArray.of((byte) 30)));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<StoreEntry> it = reader.scan(ByteArray.of((byte) 2), null);
            List<StoreEntry> entries = collectEntries(it);

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
            writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 2), ByteArray.of((byte) 20)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 3), ByteArray.of((byte) 30)));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<StoreEntry> it = reader.scan(null, ByteArray.of((byte) 3));
            List<StoreEntry> entries = collectEntries(it);

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
            writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 2), ByteArray.of((byte) 20)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 3), ByteArray.of((byte) 30)));
            writer.append(StoreEntry.of(ByteArray.of((byte) 4), ByteArray.of((byte) 40)));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<StoreEntry> it = reader.scan(ByteArray.of((byte) 2), ByteArray.of((byte) 4));
            List<StoreEntry> entries = collectEntries(it);

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
        List<StoreEntry> entries = new ArrayList<>();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (int i = 0; i < 20; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.wrap(largeValue);
                StoreEntry entry = StoreEntry.of(key, value);
                entries.add(entry);
                writer.append(entry);
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (StoreEntry entry : entries) {
                Optional<StoreEntry> result = reader.get(entry.key());
                assertThat(result).isPresent();
                assertThat(result.get().key()).isEqualTo(entry.key());
            }

            Iterator<StoreEntry> it = reader.scan(null, null);
            List<StoreEntry> scanned = collectEntries(it);
            assertThat(scanned).hasSize(20);
        }
    }

    @Test
    void scanEmptyRange() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            writer.append(StoreEntry.of(ByteArray.of((byte) 1), ByteArray.of((byte) 10)));
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            Iterator<StoreEntry> it = reader.scan(ByteArray.of((byte) 10), ByteArray.of((byte) 20));
            List<StoreEntry> entries = collectEntries(it);

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
                writer.append(StoreEntry.of(key, value));
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (int i = 100; i < 200; i++) {
                ByteArray missingKey = ByteArray.of((byte) i);
                Optional<StoreEntry> result = reader.get(missingKey);
                assertThat(result).isEmpty();
            }
        }
    }

    @Test
    void bloomFilterDoesNotCauseFalseNegatives() {
        Path sstablePath = tempDir.resolve("test.sst");
        SSTableConfig config = SSTableConfig.create();

        List<StoreEntry> entries = new ArrayList<>();
        try (SSTableWriter writer = SSTableWriter.create(sstablePath, config)) {
            for (int i = 0; i < 100; i++) {
                ByteArray key = ByteArray.of((byte) i);
                ByteArray value = ByteArray.of((byte) (i * 2));
                StoreEntry entry = StoreEntry.of(key, value);
                entries.add(entry);
                writer.append(entry);
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (StoreEntry entry : entries) {
                Optional<StoreEntry> result = reader.get(entry.key());
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
                writer.append(StoreEntry.of(key, value));
            }
        }

        try (SSTableReader reader = SSTableReader.open(sstablePath)) {
            for (int i = 0; i < 50; i++) {
                ByteArray key = ByteArray.of((byte) i);
                Optional<StoreEntry> result = reader.get(key);
                assertThat(result).isPresent();
            }
        }
    }

    private List<StoreEntry> collectEntries(Iterator<StoreEntry> iterator) {
        List<StoreEntry> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }
}
