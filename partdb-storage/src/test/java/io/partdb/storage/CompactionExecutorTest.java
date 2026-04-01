package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class CompactionExecutorTest {

    @TempDir
    Path tempDir;

    @Test
    void splitsOutputsBeforeOvershootingTargetSize() {
        LsmConfig config = LsmConfig.defaults()
            .withTargetUncompressedSize(200);

        try (SSTableCatalog store = SSTableCatalog.open(tempDir, config, new StorageRuntimeStats())) {
            SSTableMetadata input;
            try (SSTableWriter writer = store.createWriter(0)) {
                writer.add(put("key-1", value(96), 1));
                writer.add(put("key-2", value(96), 2));
                writer.add(put("key-3", value(96), 3));
                input = writer.finish();
            }

            CompactionExecutor executor = new CompactionExecutor(store, config);
            CompactionResult result = executor.compact(new CompactionTask(List.of(input), 1, false));

            CompactionResult.Success success = assertInstanceOf(CompactionResult.Success.class, result);
            List<SSTableMetadata> outputs = success.outputs();

            assertEquals(3, outputs.size());
            assertEquals(List.of(1L, 1L, 1L), outputs.stream().map(SSTableMetadata::entryCount).toList());
        }
    }

    @Test
    void splitsOutputsWhenGrandparentOverlapGetsTooLarge() {
        LsmConfig config = LsmConfig.defaults()
            .withTargetUncompressedSize(1_000);

        try (SSTableCatalog store = SSTableCatalog.open(tempDir, config, new StorageRuntimeStats())) {
            SSTableMetadata input;
            try (SSTableWriter writer = store.createWriter(0)) {
                writer.add(put("key-1", value(96), 1));
                writer.add(put("key-2", value(96), 2));
                writer.add(put("key-3", value(96), 3));
                input = writer.finish();
            }

            List<SSTableMetadata> grandparents = List.of(
                metadata(10, 2, "key-1", "key-1", 6_000),
                metadata(11, 2, "key-2", "key-2", 6_000)
            );

            CompactionExecutor executor = new CompactionExecutor(store, config);
            CompactionResult result = executor.compact(new CompactionTask(List.of(input), grandparents, 1, false));

            CompactionResult.Success success = assertInstanceOf(CompactionResult.Success.class, result);
            List<SSTableMetadata> outputs = success.outputs();

            assertEquals(2, outputs.size());
            assertEquals(List.of(1L, 2L), outputs.stream().map(SSTableMetadata::entryCount).toList());
        }
    }

    private static StoredEntry.Value put(String key, byte[] value, long revision) {
        return new StoredEntry.Value(slice(key), Slice.copyOf(value), revision);
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
}
