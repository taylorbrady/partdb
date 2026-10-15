package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Optional;

public interface StorageReadView extends AutoCloseable {

    Optional<ValueRecord> get(Bytes key);

    @Override
    void close();
}
