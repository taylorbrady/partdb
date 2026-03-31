package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public final class VersionedKeyValueStore implements AutoCloseable {

    private final StoreRuntime runtime;

    private VersionedKeyValueStore(StoreRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime must not be null");
    }

    public static VersionedKeyValueStore open(Path dataDirectory) {
        return open(dataDirectory, StorageConfig.defaults());
    }

    public static VersionedKeyValueStore open(Path dataDirectory, StorageConfig config) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        return new VersionedKeyValueStore(StoreRuntime.open(dataDirectory, config.toLsmConfig()));
    }

    public void put(Bytes key, Bytes value, long revision) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        runtime.put(Slice.of(key.toByteArray()), Slice.of(value.toByteArray()), revision);
    }

    public void delete(Bytes key, long revision) {
        Objects.requireNonNull(key, "key must not be null");
        runtime.delete(Slice.of(key.toByteArray()), revision);
    }

    public Optional<VersionedValue> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        return runtime.get(Slice.of(key.toByteArray()))
            .map(entry -> new VersionedValue(Bytes.copyOf(entry.value().toByteArray()), entry.revision()));
    }

    public EntryCursor scan(KeyRange range) {
        Objects.requireNonNull(range, "range must not be null");

        return switch (range) {
            case KeyRange.All _ ->
                new CursorAdapter(runtime.scan(null, null));
            case KeyRange.From(var startInclusive) ->
                new CursorAdapter(runtime.scan(Slice.of(startInclusive.toByteArray()), null));
            case KeyRange.Until(var endExclusive) ->
                new CursorAdapter(runtime.scan(null, Slice.of(endExclusive.toByteArray())));
            case KeyRange.Between(var startInclusive, var endExclusive) ->
                new CursorAdapter(
                    runtime.scan(
                        Slice.of(startInclusive.toByteArray()),
                        Slice.of(endExclusive.toByteArray())
                    )
                );
        };
    }

    public StorageCheckpoint checkpoint() {
        return new StorageCheckpoint(Bytes.copyOf(runtime.checkpoint()));
    }

    public void replaceWith(StorageCheckpoint checkpoint) {
        runtime.replaceWithCheckpoint(
            Objects.requireNonNull(checkpoint, "checkpoint must not be null").bytes().toByteArray()
        );
    }

    public LsmStats stats() {
        return runtime.statsSnapshot();
    }

    @Override
    public void close() {
        runtime.close();
    }

    private static final class CursorAdapter implements EntryCursor {
        private final EngineEntryCursor cursor;

        private CursorAdapter(EngineEntryCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @Override
        public VersionedEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            EngineEntry entry = cursor.next();
            return new VersionedEntry(
                Bytes.copyOf(entry.key().toByteArray()),
                Bytes.copyOf(entry.value().toByteArray()),
                entry.revision()
            );
        }

        @Override
        public void close() {
            cursor.close();
        }
    }
}
