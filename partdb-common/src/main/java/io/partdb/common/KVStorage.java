package io.partdb.common;

import java.util.Iterator;
import java.util.Optional;

public interface KVStorage extends AutoCloseable {

    void put(Entry entry);

    Optional<Entry> getEntry(ByteArray key);

    Iterator<Entry> scan(ByteArray startKey, ByteArray endKey);

    void flush();

    long lastAppliedIndex();

    void setLastAppliedIndex(long index);

    byte[] toSnapshot();

    void restoreSnapshot(byte[] data);

    @Override
    void close();
}
