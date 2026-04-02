package io.partdb.storage;

import java.util.List;
import java.util.Objects;

record LoadedStoreVersion(
    SSTableManifest manifest,
    List<SSTableReader> readers
) {

    LoadedStoreVersion {
        Objects.requireNonNull(manifest, "manifest");
        readers = List.copyOf(Objects.requireNonNull(readers, "readers"));
    }

    StoreVersion toStoreVersion() {
        return new StoreVersion(manifest, readers);
    }
}
