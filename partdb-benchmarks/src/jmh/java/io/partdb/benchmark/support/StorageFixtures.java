package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;
import io.partdb.storage.Mutation;
import io.partdb.storage.StoreOptions;
import io.partdb.storage.Store;

import java.util.List;

public final class StorageFixtures {

    private static final int KIB = 1024;

    private StorageFixtures() {
    }

    public static StoreOptions defaultOptions() {
        return StoreOptions.defaults();
    }

    public static StoreOptions compactionOptions(StoreOptions.Compression compression) {
        return StoreOptions.defaults()
            .withMemtableMaxBytes(256L * KIB)
            .withBlockCacheMaxBytes(0)
            .withTable(StoreOptions.Table.defaults().withBlockSizeBytes(16 * KIB).withCompression(compression))
            .withCompaction(new StoreOptions.Compaction(256L * KIB, 1, 2, 512L * KIB, 4, 5));
    }

    public static long populate(Store store, Bytes[] keys, Bytes value, long startingRevision) {
        long revision = startingRevision;
        for (Bytes key : keys) {
            store.apply(revision++, List.of(Mutation.put(key, value)));
        }
        return revision;
    }
}
