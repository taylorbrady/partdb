package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

public final class StorageEngine implements AutoCloseable {

    private final StorageEngineCore core;

    private StorageEngine(StorageEngineCore core) {
        this.core = Objects.requireNonNull(core, "core must not be null");
    }

    public static StorageEngine open(Path dataDirectory) {
        return open(dataDirectory, StorageConfig.defaults());
    }

    public static StorageEngine open(Path dataDirectory, StorageConfig config) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        return new StorageEngine(StorageEngineCore.open(dataDirectory, config.toLsmConfig()));
    }

    public static StorageEngine restore(Path dataDirectory, StorageCheckpoint checkpoint, StorageConfig config) {
        StorageEngine engine = open(dataDirectory, config);
        try {
            engine.restoreInPlace(checkpoint);
            return engine;
        } catch (RuntimeException e) {
            engine.close();
            throw e;
        }
    }

    public void apply(Revision revision, Mutation mutation) {
        apply(revision, WriteBatch.of(mutation));
    }

    public void apply(Revision revision, WriteBatch batch) {
        Objects.requireNonNull(revision, "revision must not be null");
        Objects.requireNonNull(batch, "batch must not be null");
        if (batch.isEmpty()) {
            return;
        }

        List<StoredEntry> entries = new ArrayList<>(batch.mutations().size());
        for (Mutation mutation : batch.mutations()) {
            entries.add(toStoredEntry(revision, mutation));
        }
        core.apply(entries);
    }

    public Optional<ValueRecord> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        return core.get(copy(key))
            .map(entry -> new ValueRecord(Bytes.copyOf(entry.value().toByteArray()), new Revision(entry.revision())));
    }

    public Scan scan(KeyRange range) {
        Objects.requireNonNull(range, "range must not be null");

        return switch (range) {
            case KeyRange.All _ ->
                new ScanAdapter(core.scan(ScanBounds.all()));
            case KeyRange.From(var startInclusive) ->
                new ScanAdapter(core.scan(ScanBounds.from(copy(startInclusive))));
            case KeyRange.Until(var endExclusive) ->
                new ScanAdapter(core.scan(ScanBounds.until(copy(endExclusive))));
            case KeyRange.Between(var startInclusive, var endExclusive) ->
                new ScanAdapter(core.scan(ScanBounds.between(copy(startInclusive), copy(endExclusive))));
        };
    }

    public StorageCheckpoint checkpoint() {
        return new StorageCheckpoint(Bytes.copyOf(core.checkpoint()));
    }

    public void restoreInPlace(StorageCheckpoint checkpoint) {
        core.replaceWithCheckpoint(
            Objects.requireNonNull(checkpoint, "checkpoint must not be null").bytes().toByteArray()
        );
    }

    public StorageMetadata metadata() {
        return core.metadataSnapshot();
    }

    public LsmStats stats() {
        return core.statsSnapshot();
    }

    @Override
    public void close() {
        core.close();
    }

    private static Slice copy(Bytes bytes) {
        return Slice.copyOf(bytes.toByteArray());
    }

    private static StoredEntry toStoredEntry(Revision revision, Mutation mutation) {
        return switch (mutation) {
            case Mutation.Put(var key, var value) -> new StoredEntry.Value(copy(key), copy(value), revision.value());
            case Mutation.Delete(var key) -> new StoredEntry.Tombstone(copy(key), revision.value());
        };
    }

    private static final class ScanAdapter implements Scan {
        private final StoredValueCursor cursor;

        private ScanAdapter(StoredValueCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @Override
        public EntryRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            StoredEntry.Value entry = cursor.next();
            return new EntryRecord(
                Bytes.copyOf(entry.key().toByteArray()),
                Bytes.copyOf(entry.value().toByteArray()),
                new Revision(entry.revision())
            );
        }

        @Override
        public void close() {
            cursor.close();
        }
    }
}
