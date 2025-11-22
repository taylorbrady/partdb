package io.partdb.storage;

import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.nio.file.Path;
import java.util.Objects;

public record LSMEngineConfig(
    Path dataDirectory,
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig
) {
    public LSMEngineConfig {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(memtableConfig, "memtableConfig must not be null");
        Objects.requireNonNull(sstableConfig, "sstableConfig must not be null");
    }

    public static LSMEngineConfig create(Path dataDirectory) {
        return new LSMEngineConfig(
            dataDirectory,
            MemtableConfig.create(),
            SSTableConfig.create()
        );
    }
}
