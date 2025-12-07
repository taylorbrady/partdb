package io.partdb.storage;

import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.util.Objects;

public record LSMConfig(
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig
) {
    public LSMConfig {
        Objects.requireNonNull(memtableConfig, "memtableConfig must not be null");
        Objects.requireNonNull(sstableConfig, "sstableConfig must not be null");
    }

    public static LSMConfig create() {
        return new LSMConfig(
            MemtableConfig.create(),
            SSTableConfig.create()
        );
    }
}
