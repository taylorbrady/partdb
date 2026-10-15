package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Optional;

public sealed interface Snapshot extends AutoCloseable permits StoreSnapshot {

    long index();

    Optional<Entry> get(Bytes key);

    Cursor scan(KeyRange range);

    @Override
    void close();
}
