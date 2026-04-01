package io.partdb.benchmark.support;

import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedKeyValueStore;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public abstract class AbstractStorageState {

    private Path tempDirectory;
    private VersionedKeyValueStore store;

    protected final void openStore(String prefix, StorageConfig config) throws IOException {
        tempDirectory = BenchmarkDirectories.createTempDirectory(prefix);
        store = VersionedKeyValueStore.open(tempDirectory, config);
    }

    protected final void reopenStore(StorageConfig config) {
        Objects.requireNonNull(tempDirectory, "tempDirectory");
        Objects.requireNonNull(store, "store");
        store.close();
        store = VersionedKeyValueStore.open(tempDirectory, config);
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

    public final VersionedKeyValueStore store() {
        return Objects.requireNonNull(store, "store has not been opened");
    }

    public final Path tempDirectory() {
        return Objects.requireNonNull(tempDirectory, "tempDirectory has not been created");
    }
}
