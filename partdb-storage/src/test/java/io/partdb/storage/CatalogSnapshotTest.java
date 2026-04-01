package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class CatalogSnapshotTest {

    @TempDir
    Path tempDir;

    @Test
    void lookupUsesLevelOrderInsteadOfReaderListOrder() {
        SSTableMetadata newerDescriptor = writeTable(
            1,
            1,
            "shared-key",
            "newer-value",
            20
        );
        SSTableMetadata olderDescriptor = writeTable(
            2,
            2,
            "shared-key",
            "older-value",
            10
        );

        SSTableReader newerReader = SSTableReader.open(
            newerDescriptor.id(),
            newerDescriptor.level(),
            tablePath(newerDescriptor.id()),
            NoOpBlockCache.INSTANCE
        );
        SSTableReader olderReader = SSTableReader.open(
            olderDescriptor.id(),
            olderDescriptor.level(),
            tablePath(olderDescriptor.id()),
            NoOpBlockCache.INSTANCE
        );

        SSTableManifest manifest = new SSTableManifest(2, 20, List.of(newerDescriptor, olderDescriptor));
        CatalogGeneration generation = new CatalogGeneration(manifest, List.of(olderReader, newerReader));
        CatalogGeneration.CatalogLease lease = generation.tryAcquire();
        if (lease == null) {
            throw new AssertionError("fresh catalog generation should be acquirable");
        }

        generation.retire(List.of());

        try (CatalogSnapshot view = new CatalogSnapshot(lease)) {
            StoredEntry mutation = view.get(slice("shared-key")).orElseThrow();

            assertInstanceOf(StoredEntry.Value.class, mutation);
            assertEquals(slice("newer-value"), ((StoredEntry.Value) mutation).value());
        }
    }

    @Test
    void scanTablesRespectManifestLevelOrder() {
        SSTableMetadata newestL0 = writeTable(1, 0, "a", "l0", 30);
        SSTableMetadata l1 = writeTable(2, 1, "a", "l1", 20);
        SSTableMetadata l2 = writeTable(3, 2, "a", "l2", 10);

        SSTableReader l2Reader = SSTableReader.open(l2.id(), l2.level(), tablePath(l2.id()), NoOpBlockCache.INSTANCE);
        SSTableReader newestL0Reader = SSTableReader.open(
            newestL0.id(),
            newestL0.level(),
            tablePath(newestL0.id()),
            NoOpBlockCache.INSTANCE
        );
        SSTableReader l1Reader = SSTableReader.open(l1.id(), l1.level(), tablePath(l1.id()), NoOpBlockCache.INSTANCE);

        SSTableManifest manifest = new SSTableManifest(3, 30, List.of(newestL0, l1, l2));
        CatalogGeneration generation = new CatalogGeneration(manifest, List.of(l2Reader, newestL0Reader, l1Reader));
        CatalogGeneration.CatalogLease lease = generation.tryAcquire();
        if (lease == null) {
            throw new AssertionError("fresh catalog generation should be acquirable");
        }

        generation.retire(List.of());

        try (CatalogSnapshot view = new CatalogSnapshot(lease)) {
            List<SSTableReader> tables = view.scanTables(ScanBounds.between(slice("a"), slice("z")));

            assertEquals(List.of(newestL0Reader, l1Reader, l2Reader), tables);
        }
    }

    private SSTableMetadata writeTable(long id, int level, String key, String value, long revision) {
        try (SSTableWriter writer = SSTableWriter.create(id, level, tablePath(id), LsmConfig.defaults())) {
            writer.add(new StoredEntry.Value(slice(key), slice(value), revision));
            return writer.finish();
        }
    }

    private Path tablePath(long id) {
        return tempDir.resolve("%06d.sst".formatted(id));
    }

    private static Slice slice(String value) {
        return Slice.utf8(value);
    }
}
