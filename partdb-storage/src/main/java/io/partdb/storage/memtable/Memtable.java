package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.util.Iterator;
import java.util.Optional;

public interface Memtable {

    void put(Entry entry);

    Optional<Entry> get(ByteArray key);

    Iterator<Entry> scan(ByteArray startKey, ByteArray endKey);

    long sizeInBytes();

    long entryCount();

    void clear();
}
