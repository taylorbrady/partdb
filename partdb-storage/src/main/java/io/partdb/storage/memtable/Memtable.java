package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.storage.StoreEntry;

import java.util.Optional;

public interface Memtable {

    void put(StoreEntry entry);

    Optional<StoreEntry> get(ByteArray key);

    CloseableIterator<StoreEntry> scan(ByteArray startKey, ByteArray endKey);

    long sizeInBytes();

    long entryCount();

    void clear();
}
