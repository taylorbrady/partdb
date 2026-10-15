package io.partdb.storage;

import java.util.Iterator;

public interface Scan extends Iterable<EntryRecord>, AutoCloseable {

    @Override
    Iterator<EntryRecord> iterator();

    @Override
    void close();
}
