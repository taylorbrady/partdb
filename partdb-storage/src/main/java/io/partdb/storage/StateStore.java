package io.partdb.storage;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

/**
 * Versioned key-value state store used by PartDB's replicated state machine.
 * Revisions are caller-assigned and must be monotonic per key. Replaying the
 * same mutation at the same revision is allowed, but conflicting or stale
 * revisions are rejected.
 */
public interface StateStore extends AutoCloseable {

    static StateStore open(Path dataDirectory) {
        return open(dataDirectory, StorageConfig.defaults());
    }

    static StateStore open(Path dataDirectory, StorageConfig config) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        return new LsmStateStore(LsmEngine.open(dataDirectory, config.toLsmConfig()));
    }

    /**
     * Stores a value at a caller-assigned revision that must be greater than
     * the currently visible revision for the key, unless this is an identical
     * replay of the same mutation.
     */
    void put(byte[] key, byte[] value, long revision);

    /**
     * Deletes a key at a caller-assigned revision that must be greater than
     * the currently visible revision for the key, unless this is an identical
     * replay of the same mutation.
     */
    void delete(byte[] key, long revision);

    Optional<VersionedEntry> get(byte[] key);

    StorageCursor scan(byte[] startKeyInclusive, byte[] endKeyExclusive);

    StorageSnapshot snapshot();

    void restore(StorageSnapshot snapshot);

    @Override
    void close();
}
