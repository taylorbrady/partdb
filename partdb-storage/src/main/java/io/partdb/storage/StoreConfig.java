package io.partdb.storage;

import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.util.Objects;

public record StoreConfig(
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig
) {
    public StoreConfig {
        Objects.requireNonNull(memtableConfig, "memtableConfig must not be null");
        Objects.requireNonNull(sstableConfig, "sstableConfig must not be null");
    }

    public static StoreConfig create() {
        return new StoreConfig(
            MemtableConfig.create(),
            SSTableConfig.create()
        );
    }
}
