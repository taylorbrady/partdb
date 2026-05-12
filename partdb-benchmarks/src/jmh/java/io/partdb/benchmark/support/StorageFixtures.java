package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;
import io.partdb.storage.CacheOptions;
import io.partdb.storage.CompactionOptions;
import io.partdb.storage.Mutation;
import io.partdb.storage.Revision;
import io.partdb.storage.SstableOptions;
import io.partdb.storage.StorageOptions;
import io.partdb.storage.StorageEngine;

public final class StorageFixtures {

    private static final int KIB = 1024;
    private static final int MIB = 1024 * KIB;

    private StorageFixtures() {
    }

    public static StorageOptions defaultOptions() {
        return StorageOptions.defaults();
    }

    public static StorageOptions compactionOptions() {
        return compactionOptions(SstableOptions.Compression.NONE);
    }

    public static StorageOptions compactionOptions(SstableOptions.Compression compression) {
        return StorageOptions.builder()
            .writeBufferMaxBytes(256L * KIB)
            .cacheOptions(CacheOptions.builder()
                .blockCacheMaxBytes(0)
                .build())
            .sstableOptions(SstableOptions.builder()
                .blockSizeBytes(16 * KIB)
                .compression(compression)
                .build())
            .compactionOptions(CompactionOptions.builder()
                .targetTableSizeBytes(256L * KIB)
                .maxConcurrentCompactions(1)
                .l0CompactionTrigger(2)
                .maxBytesForLevelBase(512L * KIB)
                .levelMultiplier(4)
                .maxLevels(5)
                .build())
            .build();
    }

    public static long populate(StorageEngine store, Bytes[] keys, Bytes value, long startingRevision) {
        long revision = startingRevision;
        for (Bytes key : keys) {
            store.apply(new Revision(revision++), Mutation.put(key, value));
        }
        return revision;
    }
}
