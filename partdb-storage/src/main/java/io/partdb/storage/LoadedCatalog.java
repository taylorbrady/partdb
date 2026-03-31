package io.partdb.storage;

import java.util.List;
import java.util.Objects;

record LoadedCatalog(
    SSTableManifest manifest,
    List<SSTableReader> readers
) {

    LoadedCatalog {
        Objects.requireNonNull(manifest, "manifest");
        readers = List.copyOf(Objects.requireNonNull(readers, "readers"));
    }

    CatalogGeneration toGeneration() {
        return new CatalogGeneration(manifest, readers);
    }
}
