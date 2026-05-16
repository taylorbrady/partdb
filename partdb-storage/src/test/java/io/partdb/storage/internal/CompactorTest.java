package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CompactorTest {

    @TempDir
    Path tempDir;

    @Test
    void splitsOutputsBeforeOvershootingTargetSize() {
        LsmConfig config = LsmConfig.defaults()
            .withTargetUncompressedSize(200);

        try (CompactorFixture fixture = openFixture(config)) {
            SSTableMetadata input;
            try (SSTableWriter writer = fixture.createWriter(0)) {
                writer.add(put("key-1", value(96), 1));
                writer.add(put("key-2", value(96), 2));
                writer.add(put("key-3", value(96), 3));
                input = writer.finish();
            }

            List<SSTableMetadata> outputs = fixture.compactor().compact(new CompactionTask(List.of(input), 1, false));

            assertEquals(3, outputs.size());
            assertEquals(List.of(1L, 1L, 1L), outputs.stream().map(SSTableMetadata::entryCount).toList());
        }
    }

    @Test
    void splitsOutputsWhenGrandparentOverlapGetsTooLarge() {
        LsmConfig config = LsmConfig.defaults()
            .withTargetUncompressedSize(1_000);

        try (CompactorFixture fixture = openFixture(config)) {
            SSTableMetadata input;
            try (SSTableWriter writer = fixture.createWriter(0)) {
                writer.add(put("key-1", value(96), 1));
                writer.add(put("key-2", value(96), 2));
                writer.add(put("key-3", value(96), 3));
                input = writer.finish();
            }

            List<SSTableMetadata> grandparents = List.of(
                metadata(10, 2, "key-1", "key-1", 6_000),
                metadata(11, 2, "key-2", "key-2", 6_000)
            );

            List<SSTableMetadata> outputs = fixture.compactor().compact(new CompactionTask(List.of(input), grandparents, 1, false));

            assertEquals(2, outputs.size());
            assertEquals(List.of(1L, 2L), outputs.stream().map(SSTableMetadata::entryCount).toList());
        }
    }

    @Test
    void dropsShadowedVersionsWhenNoSnapshotsAreOpen() {
        try (CompactorFixture fixture = openFixture(LsmConfig.defaults())) {
            SSTableMetadata input;
            try (SSTableWriter writer = fixture.createWriter(0)) {
                writer.add(put("key", value(8), 5));
                writer.add(put("key", value(8), 3));
                writer.add(put("key", value(8), 1));
                input = writer.finish();
            }

            List<SSTableMetadata> outputs = fixture.compactor().compact(new CompactionTask(List.of(input), 1, false));
            assertEquals(List.of("key@5:value"), readEntries(fixture, outputs));
        }
    }

    @Test
    void retainsBoundaryVersionNeededByOldestOpenSnapshot() {
        try (CompactorFixture fixture = openFixture(LsmConfig.defaults());
             VersionLease snapshot = fixture.versionSet().acquire(4)) {
            SSTableMetadata input;
            try (SSTableWriter writer = fixture.createWriter(0)) {
                writer.add(put("key", value(8), 5));
                writer.add(put("key", value(8), 3));
                writer.add(put("key", value(8), 1));
                input = writer.finish();
            }

            List<SSTableMetadata> outputs = fixture.compactor().compact(new CompactionTask(List.of(input), 1, false));
            assertEquals(List.of("key@5:value", "key@3:value"), readEntries(fixture, outputs));
        }
    }

    @Test
    void dropsBottommostTombstoneWhenNoSnapshotsNeedIt() {
        try (CompactorFixture fixture = openFixture(LsmConfig.defaults())) {
            SSTableMetadata input;
            try (SSTableWriter writer = fixture.createWriter(3)) {
                writer.add(delete("key", 5));
                writer.add(put("key", value(8), 3));
                input = writer.finish();
            }

            List<SSTableMetadata> outputs = fixture.compactor().compact(new CompactionTask(List.of(input), 4, true));
            assertEquals(List.of(), outputs);
        }
    }

    @Test
    void retainsBottommostTombstoneWhileOldSnapshotStillNeedsPriorValue() {
        try (CompactorFixture fixture = openFixture(LsmConfig.defaults());
             VersionLease snapshot = fixture.versionSet().acquire(4)) {
            SSTableMetadata input;
            try (SSTableWriter writer = fixture.createWriter(3)) {
                writer.add(delete("key", 5));
                writer.add(put("key", value(8), 3));
                input = writer.finish();
            }

            List<SSTableMetadata> outputs = fixture.compactor().compact(new CompactionTask(List.of(input), 4, true));
            assertEquals(List.of("key@5:tombstone", "key@3:value"), readEntries(fixture, outputs));
        }
    }

    private static StoredEntry.Value put(String key, byte[] value, long revision) {
        return new StoredEntry.Value(slice(key), Slice.copyOf(value), revision);
    }

    private static StoredEntry.Tombstone delete(String key, long revision) {
        return new StoredEntry.Tombstone(slice(key), revision);
    }

    private static Slice slice(String value) {
        return Slice.utf8(value);
    }

    private static SSTableMetadata metadata(long id, int level, String smallest, String largest, long fileSizeBytes) {
        return new SSTableMetadata(
            id,
            level,
            slice(smallest),
            slice(largest),
            id,
            id,
            fileSizeBytes,
            1
        );
    }

    private static byte[] value(int size) {
        return new byte[size];
    }

    private CompactorFixture openFixture(LsmConfig config) {
        ManifestStore manifestStore = new ManifestStore(tempDir);
        SSTableStore sstableStore = new SSTableStore(tempDir, config, NoOpBlockCache.INSTANCE);
        StorageStatsCollector stats = new StorageStatsCollector();
        LoadedStoreVersion initialState = sstableStore.openState(manifestStore);
        VersionSet versionSet = VersionSet.open(manifestStore, sstableStore, initialState, stats);
        return new CompactorFixture(
            sstableStore,
            versionSet,
            new Compactor(sstableStore, versionSet, config)
        );
    }

    private static List<String> readEntries(CompactorFixture fixture, List<SSTableMetadata> tables) {
        List<String> entries = new ArrayList<>();
        for (SSTableMetadata table : tables) {
            try (SSTableReader reader = fixture.sstableStore().openReader(table)) {
                Iterator<InternalEntry> cursor = reader.scan(ScanBounds.all());
                while (cursor.hasNext()) {
                    InternalEntry entry = cursor.next();
                    entries.add("%s@%d:%s".formatted(
                        utf8(entry.userKey()),
                        entry.revision(),
                        entry instanceof InternalEntry.Tombstone ? "tombstone" : "value"
                    ));
                }
            }
        }
        return entries;
    }

    private static String utf8(Slice value) {
        return new String(value.toByteArray(), StandardCharsets.UTF_8);
    }

    private record CompactorFixture(
        SSTableStore sstableStore,
        VersionSet versionSet,
        Compactor compactor
    ) implements AutoCloseable {

        private SSTableWriter createWriter(int level) {
            return sstableStore.createWriter(versionSet.allocateSstableId(), level);
        }

        @Override
        public void close() {
            StoreVersion finalVersion = versionSet.close();
            if (!finalVersion.awaitDrain(Duration.ofSeconds(1))) {
                finalVersion.forceDrain();
            }
        }
    }
}
