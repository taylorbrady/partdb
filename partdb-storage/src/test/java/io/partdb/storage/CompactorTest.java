package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class CompactorTest {

    @TempDir
    Path tempDir;

    @Test
    void splitsOutputsBeforeOvershootingTargetSize() {
        LSMConfig config = LSMConfig.defaults()
            .withTargetUncompressedSize(200);

        try (SSTableStore store = SSTableStore.open(tempDir, config)) {
            SSTableDescriptor input;
            try (SSTable.Builder builder = store.createBuilder(0)) {
                builder.add(put("key-1", value(96), 1));
                builder.add(put("key-2", value(96), 2));
                builder.add(put("key-3", value(96), 3));
                input = builder.finish();
            }

            Compactor compactor = new Compactor(store, config);
            CompactionResult result = compactor.compact(new CompactionTask(List.of(input), 1, false));

            CompactionResult.Success success = assertInstanceOf(CompactionResult.Success.class, result);
            List<SSTableDescriptor> outputs = success.outputs();

            assertEquals(3, outputs.size());
            assertEquals(List.of(1L, 1L, 1L), outputs.stream().map(SSTableDescriptor::entryCount).toList());
        }
    }

    private static Mutation.Put put(String key, byte[] value, long revision) {
        return new Mutation.Put(slice(key), Slice.of(value), revision);
    }

    private static Slice slice(String value) {
        return Slice.of(value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] value(int size) {
        return new byte[size];
    }
}
