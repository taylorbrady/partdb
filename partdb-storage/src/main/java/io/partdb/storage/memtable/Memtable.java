package io.partdb.storage.memtable;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.ScanMode;

import java.util.Iterator;
import java.util.Optional;

public interface Memtable {

    void put(Entry entry);

    Optional<Entry> get(ByteArray key, Timestamp readTimestamp);

    Iterator<Entry> scan(ScanMode mode, ByteArray startKey, ByteArray endKey);

    long sizeInBytes();

    long entryCount();

    void clear();
}
