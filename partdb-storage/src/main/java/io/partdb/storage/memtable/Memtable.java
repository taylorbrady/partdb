package io.partdb.storage.memtable;

import io.partdb.common.Slice;
import io.partdb.storage.Mutation;

import java.util.Iterator;
import java.util.Optional;

public interface Memtable {

    void put(Mutation mutation);

    Optional<Mutation> get(Slice key);

    Iterator<Mutation> scan(Slice startKey, Slice endKey);

    long sizeInBytes();

    long entryCount();

    void clear();
}
