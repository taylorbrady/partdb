package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

final class Fixtures {

    static final StoreOptions SMALL = StoreOptions.defaults()
        .withMemtableBytes(4 * 1024)
        .withBlockCacheBytes(1024 * 1024)
        .withBlockBytes(512)
        .withTargetTableBytes(8 * 1024)
        .withLevelZeroTriggers(2, 64);

    static final StoreOptions NO_COMPACTION = SMALL.withLevelZeroTriggers(1_000_000, Integer.MAX_VALUE);

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
        try (Snapshot snapshot = store.snapshot()) {
            return value(snapshot, key);
        }
    }

    static String value(Snapshot snapshot, String key) {
        return snapshot.get(bytes(key)).map(entry -> entry.value().utf8()).orElse(null);
    }

    static List<String> keys(Snapshot snapshot, KeyRange range) {
        try (Stream<KeyValue> entries = snapshot.scan(range)) {
            return entries.map(entry -> entry.key().utf8()).toList();
        }
    }

    static List<KeyValue> entries(Store store) {
        try (Snapshot snapshot = store.snapshot(); Stream<KeyValue> entries = snapshot.scan(KeyRange.all())) {
            return entries.toList();
        }
    }

    static byte[] checkpoint(Store store) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        store.checkpoint(out);
        return out.toByteArray();
    }

    static long restore(Store store, byte[] checkpoint) {
        return store.restore(new ByteArrayInputStream(checkpoint));
    }

    static long fill(Store store, long fromRevision, int count, String valuePrefix) {
        long revision = fromRevision;
        for (int i = 0; i < count; i++) {
            store.apply(++revision, List.of(put(i, valuePrefix + i)));
        }
        return revision;
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
