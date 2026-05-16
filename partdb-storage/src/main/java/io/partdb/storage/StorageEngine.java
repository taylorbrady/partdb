package io.partdb.storage;

import io.partdb.bytes.Bytes;
import io.partdb.storage.internal.LsmStorageEngine;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public final class StorageEngine implements AutoCloseable {

    private final LsmStorageEngine core;

    private StorageEngine(LsmStorageEngine core) {
        this.core = Objects.requireNonNull(core, "core must not be null");
    }

    public static StorageEngine open(Path dataDirectory) {
        return new StorageEngine(LsmStorageEngine.open(dataDirectory));
    }

    public static StorageEngine open(Path dataDirectory, StorageOptions options) {
        return new StorageEngine(LsmStorageEngine.open(dataDirectory, options));
    }

    public static StorageEngine openFromCheckpoint(
        Path dataDirectory,
        StorageCheckpoint checkpoint,
        StorageOptions options
    ) {
        return new StorageEngine(LsmStorageEngine.openFromCheckpoint(dataDirectory, checkpoint, options));
    }

    public void apply(Revision revision, Mutation mutation) {
        core.apply(revision, mutation);
    }

    public void apply(Revision revision, MutationBatch batch) {
        core.apply(revision, batch);
    }

    public Optional<ValueRecord> get(Bytes key) {
        return core.get(key);
    }

    public Scan scan(KeyRange range) {
        return core.scan(range);
    }

    public StorageReadView readView() {
        return core.readView();
    }

    public StorageCheckpoint checkpoint() {
        return core.checkpoint();
    }

    public void restore(StorageCheckpoint checkpoint) {
        core.restore(checkpoint);
    }

    public StorageMetadata metadata() {
        return core.metadata();
    }

    public StorageStats stats() {
        return core.stats();
    }

    @Override
    public void close() {
        core.close();
    }
}
