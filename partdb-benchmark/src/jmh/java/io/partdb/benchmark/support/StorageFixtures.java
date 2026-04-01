package io.partdb.benchmark.support;

import io.partdb.bytes.Bytes;
import io.partdb.storage.StorageConfig;
import io.partdb.storage.VersionedKeyValueStore;

public final class StorageFixtures {

    private StorageFixtures() {
    }

    public static StorageConfig defaultConfig() {
        return StorageConfig.defaults();
    }

    public static long populate(VersionedKeyValueStore store, Bytes[] keys, Bytes value, long startingRevision) {
        long revision = startingRevision;
        for (Bytes key : keys) {
            store.put(key, value, revision++);
        }
        return revision;
    }
}
