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

        SSTable newerReader = SSTable.open(
            newerDescriptor.id(),
            newerDescriptor.level(),
            tablePath(newerDescriptor.id()),
            NoOpBlockCache.INSTANCE
        );
        SSTable olderReader = SSTable.open(
            olderDescriptor.id(),
            olderDescriptor.level(),
            tablePath(olderDescriptor.id()),
            NoOpBlockCache.INSTANCE
        );

        SSTableSetRef ref = SSTableSetRef.of(List.of(olderReader, newerReader));
        SSTableSetRef acquired = switch (ref.tryAcquire()) {
            case SSTableSetRef.AcquireResult.Success(var sstableSet) -> sstableSet;
            case SSTableSetRef.AcquireResult.Retired _ ->
                throw new AssertionError("fresh SSTableSetRef should be acquirable");
        };

        ref.retire(List.of(olderReader, newerReader));

        SSTableManifest manifest = new SSTableManifest(2, List.of(newerDescriptor, olderDescriptor));
        try (CatalogSnapshot view = new CatalogSnapshot(acquired, manifest)) {
            Mutation mutation = view.get(slice("shared-key")).orElseThrow();

            assertInstanceOf(Mutation.Put.class, mutation);
            assertEquals(slice("newer-value"), ((Mutation.Put) mutation).value());
        }
    }

    @Test
    void scanTablesRespectManifestLevelOrder() {
        SSTableMetadata newestL0 = writeTable(1, 0, "a", "l0", 30);
        SSTableMetadata l1 = writeTable(2, 1, "a", "l1", 20);
        SSTableMetadata l2 = writeTable(3, 2, "a", "l2", 10);

        SSTable l2Reader = SSTable.open(l2.id(), l2.level(), tablePath(l2.id()), NoOpBlockCache.INSTANCE);
        SSTable newestL0Reader = SSTable.open(
            newestL0.id(),
            newestL0.level(),
            tablePath(newestL0.id()),
            NoOpBlockCache.INSTANCE
        );
        SSTable l1Reader = SSTable.open(l1.id(), l1.level(), tablePath(l1.id()), NoOpBlockCache.INSTANCE);

        SSTableSetRef ref = SSTableSetRef.of(List.of(l2Reader, newestL0Reader, l1Reader));
        SSTableSetRef acquired = switch (ref.tryAcquire()) {
            case SSTableSetRef.AcquireResult.Success(var sstableSet) -> sstableSet;
            case SSTableSetRef.AcquireResult.Retired _ ->
                throw new AssertionError("fresh SSTableSetRef should be acquirable");
        };

        ref.retire(List.of(l2Reader, newestL0Reader, l1Reader));

        SSTableManifest manifest = new SSTableManifest(3, List.of(newestL0, l1, l2));
        try (CatalogSnapshot view = new CatalogSnapshot(acquired, manifest)) {
            List<SSTable> tables = view.scanTables(slice("a"), slice("z"));

            assertEquals(List.of(newestL0Reader, l1Reader, l2Reader), tables);
        }
    }

    private SSTableMetadata writeTable(long id, int level, String key, String value, long revision) {
        try (SSTable.Builder builder = SSTable.builder(id, level, tablePath(id), LsmConfig.defaults())) {
            builder.add(new Mutation.Put(slice(key), slice(value), revision));
            return builder.finish();
        }
    }

    private Path tablePath(long id) {
        return tempDir.resolve("%06d.sst".formatted(id));
    }

    private static Slice slice(String value) {
        return Slice.of(value.getBytes(StandardCharsets.UTF_8));
    }
}
