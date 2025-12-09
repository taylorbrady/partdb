package io.partdb.storage.memtable;

import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.ScanMode;
import io.partdb.storage.Slice;

import java.util.Iterator;
import java.util.Optional;

public interface Memtable {

    void put(Entry entry);

    Optional<Entry> get(Slice key, Timestamp readTimestamp);

    Iterator<Entry> scan(ScanMode mode, Slice startKey, Slice endKey);

    long sizeInBytes();

    long entryCount();

    void clear();
}
