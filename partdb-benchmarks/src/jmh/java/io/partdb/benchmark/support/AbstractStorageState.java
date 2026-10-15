package io.partdb.benchmark.support;

import io.partdb.storage.StoreOptions;
import io.partdb.storage.Store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

public abstract class AbstractStorageState {

    private Path tempDirectory;
    private Store store;

    protected final void openStore(String prefix, StoreOptions options) throws IOException {
        tempDirectory = BenchmarkDirectories.createTempDirectory(prefix);
        store = Store.open(tempDirectory, options);
    }

    protected final void reopenStore(StoreOptions options) {
        Objects.requireNonNull(tempDirectory, "tempDirectory");
        Objects.requireNonNull(store, "store");
        store.close();
        store = Store.open(tempDirectory, options);
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

    public final Store store() {
        return Objects.requireNonNull(store, "store has not been opened");
    }
}
