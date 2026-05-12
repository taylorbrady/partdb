package io.partdb.benchmark.support;

import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageEngine;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public abstract class AbstractStorageState {

    private Path tempDirectory;
    private StorageEngine store;

    protected final void openStore(String prefix, StorageOptions options) throws IOException {
        tempDirectory = BenchmarkDirectories.createTempDirectory(prefix);
        store = StorageEngine.open(tempDirectory, options);
    }

    protected final void reopenStore(StorageOptions options) {
        Objects.requireNonNull(tempDirectory, "tempDirectory");
        Objects.requireNonNull(store, "store");
        store.close();
        store = StorageEngine.open(tempDirectory, options);
    }

    protected final void closeStore() {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    protected final void closeAndDelete() throws IOException {
        closeStore();
        BenchmarkDirectories.deleteRecursively(tempDirectory);
        tempDirectory = null;
    }

    public final StorageEngine store() {
        return Objects.requireNonNull(store, "store has not been opened");
    }

    public final Path tempDirectory() {
        return Objects.requireNonNull(tempDirectory, "tempDirectory has not been created");
    }
}
