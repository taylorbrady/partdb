package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;
import io.partdb.storage.KeyValue;
import io.partdb.storage.Mutation;
import io.partdb.storage.Snapshot;
import io.partdb.storage.StoreOptions;
import io.partdb.storage.Store;

import java.util.List;
import java.util.Optional;

public final class StorageFixtures {

    private static final int KIB = 1024;

    private StorageFixtures() {
    }

    public static StoreOptions defaultOptions() {
        return StoreOptions.defaults();
    }

    public static StoreOptions compactionOptions(StoreOptions.Compression compression) {
        return StoreOptions.defaults()
            .withMemtableBytes(256L * KIB)
            .withBlockCacheBytes(0)
            .withBlockBytes(16 * KIB)
            .withCompression(compression)
            .withTargetTableBytes(256L * KIB)
            .withLevelZeroTriggers(2, 12)
            .withCompactionConcurrency(1);
    }

    public static Optional<KeyValue> get(Store store, Bytes key) {
        try (Snapshot snapshot = store.snapshot()) {
            return snapshot.get(key);
        }
    }

    public static long populate(Store store, Bytes[] keys, Bytes value, long startingRevision) {
        long revision = startingRevision;
        for (Bytes key : keys) {
            store.apply(revision++, List.of(Mutation.put(key, value)));
        }
        return revision;
    }
}
