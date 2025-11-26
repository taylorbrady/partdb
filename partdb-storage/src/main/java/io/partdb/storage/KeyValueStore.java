package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.KeyValue;

import java.util.Optional;

public interface KeyValueStore extends AutoCloseable {

    void put(ByteArray key, ByteArray value);

    Optional<ByteArray> get(ByteArray key);

    CloseableIterator<KeyValue> scan(ByteArray startKey, ByteArray endKey);

    void delete(ByteArray key);

    void flush();

    byte[] snapshot();

    void restore(byte[] snapshot);

    @Override
    void close();
}
