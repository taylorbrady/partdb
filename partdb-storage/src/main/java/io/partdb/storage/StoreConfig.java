package io.partdb.storage;

import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.nio.file.Path;
import java.util.Objects;

public record StoreConfig(
    Path dataDirectory,
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig
) {
    public StoreConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(memtableConfig, "memtableConfig must not be null");
        Objects.requireNonNull(sstableConfig, "sstableConfig must not be null");
    }

    public static StoreConfig create(Path dataDirectory) {
        return new StoreConfig(
            dataDirectory,
            MemtableConfig.create(),
            SSTableConfig.create()
        );
    }
}
