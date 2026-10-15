package io.partdb.storage;

import io.partdb.bytes.Bytes;
import io.partdb.storage.internal.LsmStorageEngine;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public final class StorageEngine implements AutoCloseable {

    private final LsmStorageEngine core;

    private StorageEngine(LsmStorageEngine core) {
        this.core = Objects.requireNonNull(core, "core must not be null");
    }

    public static StorageEngine open(Path dataDirectory, StorageOptions options) {
        return new StorageEngine(LsmStorageEngine.open(dataDirectory, options));
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

    public void flush() {
        core.flush();
    }

    public Revision checkpoint(OutputStream target) {
        return core.checkpoint(target);
    }

    public void restore(InputStream source) {
        core.restore(source);
    }

    public Revision appliedThrough() {
        return core.appliedThrough();
    }

    public StorageStats stats() {
        return core.stats();
    }

    @Override
    public void close() {
        core.close();
    }
}
