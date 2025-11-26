package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.util.Optional;

public interface KVStore extends AutoCloseable {

    void put(Entry entry);

    Optional<Entry> get(ByteArray key);

    CloseableIterator<Entry> scan(ByteArray startKey, ByteArray endKey);

    void flush();

    @Override
    void close();
}
