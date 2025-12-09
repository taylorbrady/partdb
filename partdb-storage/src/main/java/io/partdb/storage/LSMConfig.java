package io.partdb.storage;

import io.partdb.storage.compaction.CompactionConfig;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.SSTableConfig;

import java.util.Objects;

public record LSMConfig(
    MemtableConfig memtableConfig,
    SSTableConfig sstableConfig,
    CompactionConfig compactionConfig,
    LeveledCompactionConfig leveledCompactionConfig
) {
    public LSMConfig {
        Objects.requireNonNull(memtableConfig, "memtableConfig");
        Objects.requireNonNull(sstableConfig, "sstableConfig");
        Objects.requireNonNull(compactionConfig, "compactionConfig");
        Objects.requireNonNull(leveledCompactionConfig, "leveledCompactionConfig");
    }

    public static LSMConfig defaults() {
        return new LSMConfig(
            MemtableConfig.defaults(),
            SSTableConfig.defaults(),
            CompactionConfig.defaults(),
            LeveledCompactionConfig.defaults()
        );
    }
}
