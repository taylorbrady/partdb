package io.partdb.storage;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;

public interface StateStore extends AutoCloseable {

    static StateStore open(Path dataDirectory) {
        return open(dataDirectory, StorageConfig.defaults());
    }

    static StateStore open(Path dataDirectory, StorageConfig config) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        return new LsmStateStore(LsmEngine.open(dataDirectory, config.toLsmConfig()));
    }

    void put(byte[] key, byte[] value, long revision);

    void delete(byte[] key, long revision);

    Optional<VersionedEntry> get(byte[] key);

    StorageCursor scan(byte[] startKeyInclusive, byte[] endKeyExclusive);

    StorageSnapshot snapshot();

    void restore(StorageSnapshot snapshot);

    @Override
    void close();
}
