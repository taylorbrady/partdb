package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

final class Fixtures {

    static final StoreOptions SMALL = StoreOptions.defaults()
        .withMemtableMaxBytes(4 * 1024)
        .withBlockCacheMaxBytes(1024 * 1024)
        .withTable(StoreOptions.Table.defaults().withBlockSizeBytes(512))
        .withCompaction(new StoreOptions.Compaction(8 * 1024, 2, 2, 16 * 1024, 4, 4));

    static final StoreOptions NO_COMPACTION = SMALL
        .withCompaction(new StoreOptions.Compaction(8 * 1024, 1, 1_000_000, Long.MAX_VALUE / 16, 4, 4));

    private Fixtures() {
    }

    static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    static Mutation put(String key, String value) {
        return Mutation.put(bytes(key), bytes(value));
    }

    static Mutation delete(String key) {
        return Mutation.delete(bytes(key));
    }

    static String key(int i) {
        return "key-%06d".formatted(i);
    }

    static Mutation.Put put(int key, String value) {
        return Mutation.put(bytes(key(key)), bytes(value));
    }

    static String valueOf(InternalEntry entry) {
        return switch (entry) {
            case InternalEntry.Value value -> value.value().toBytes().utf8();
            case InternalEntry.Tombstone _ -> null;
        };
    }

    static String value(Store store, String key) {
        return store.get(bytes(key)).map(entry -> entry.value().utf8()).orElse(null);
    }

    static String value(Snapshot snapshot, String key) {
        return snapshot.get(bytes(key)).map(entry -> entry.value().utf8()).orElse(null);
    }

    static List<String> keys(Snapshot snapshot, KeyRange range) {
        List<String> keys = new ArrayList<>();
        try (Cursor cursor = snapshot.scan(range)) {
            cursor.forEachRemaining(entry -> keys.add(entry.key().utf8()));
        }
        return keys;
    }

    static List<KeyValue> entries(Store store) {
        List<KeyValue> entries = new ArrayList<>();
        try (Snapshot snapshot = store.snapshot(); Cursor cursor = snapshot.scan(KeyRange.all())) {
            cursor.forEachRemaining(entries::add);
        }
        return entries;
    }

    static byte[] checkpoint(Store store) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store.checkpoint(out);
        return out.toByteArray();
    }

    static long restore(Store store, byte[] checkpoint) {
        return store.restore(new ByteArrayInputStream(checkpoint));
    }

    static long fill(Store store, long fromIndex, int count, String valuePrefix) {
        long index = fromIndex;
        for (int i = 0; i < count; i++) {
            store.apply(++index, List.of(put(i, valuePrefix + i)));
        }
        return index;
    }

    static void await(String description, BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for " + description);
            }
            LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
        }
    }

    static Path generation(Path root) {
        try (Stream<Path> paths = Files.list(root)) {
            return paths.filter(path -> path.getFileName().toString().startsWith("gen-")).findFirst().orElseThrow();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<String> fileNames(Path directory) {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.map(path -> path.getFileName().toString()).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static List<String> tableFiles(Path directory) {
        return fileNames(directory).stream().filter(name -> name.endsWith(".sst")).toList();
    }
}
