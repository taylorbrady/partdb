package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedKeyValueStore;

public final class StorageFixtures {

    private static final int KIB = 1024;
    private static final int MIB = 1024 * KIB;

    private StorageFixtures() {
    }

    public static StorageConfig defaultConfig() {
        return StorageConfig.defaults();
    }

    public static StorageConfig compactionConfig() {
        return compactionConfig(StorageConfig.Compression.NONE);
    }

    public static StorageConfig compactionConfig(StorageConfig.Compression compression) {
        return StorageConfig.builder()
            .writeBufferMaxBytes(256L * KIB)
            .readCacheMaxBytes(0)
            .compression(compression)
            .lsmTuning(StorageConfig.LsmTuning.builder()
                .dataBlockSizeBytes(16 * KIB)
                .targetTableSizeBytes(256L * KIB)
                .maxConcurrentCompactions(1)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(512L * KIB)
                .levelMultiplier(4)
                .maxLevels(5)
                .build())
            .build();
    }

    public static long populate(VersionedKeyValueStore store, Bytes[] keys, Bytes value, long startingRevision) {
        long revision = startingRevision;
        for (Bytes key : keys) {
            store.put(key, value, revision++);
        }
        return revision;
    }
}
