package io.partdb.storage;

import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.util.Objects;

public record LSMConfig(
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig
) {

    public LSMConfig {
        Objects.requireNonNull(memtableConfig, "memtableConfig");
        Objects.requireNonNull(sstableConfig, "sstableConfig");
    }

    public static LSMConfig defaults() {
        return new LSMConfig(
            MemtableConfig.defaults(),
            SSTableConfig.defaults()
        );
    }

    public LSMConfig withMemtableConfig(MemtableConfig memtableConfig) {
        return new LSMConfig(memtableConfig, sstableConfig);
    }

    public LSMConfig withSSTableConfig(SSTableConfig sstableConfig) {
        return new LSMConfig(memtableConfig, sstableConfig);
    }
}
