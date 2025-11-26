package io.partdb.storage;

import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.util.Objects;

public record StoreConfig(
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig,
    CompactionFilter compactionFilter
) {
    public StoreConfig {
        Objects.requireNonNull(memtableConfig, "memtableConfig must not be null");
        Objects.requireNonNull(sstableConfig, "sstableConfig must not be null");
        Objects.requireNonNull(compactionFilter, "compactionFilter must not be null");
    }

    public static StoreConfig create() {
        return new StoreConfig(
            MemtableConfig.create(),
            SSTableConfig.create(),
            CompactionFilter.retainAll()
        );
    }

    public static StoreConfig withFilter(CompactionFilter filter) {
        return new StoreConfig(
            MemtableConfig.create(),
            SSTableConfig.create(),
            filter
        );
    }
}
