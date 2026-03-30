package io.partdb.storage;

import java.util.Iterator;

public interface StorageCursor extends Iterator<VersionedEntry>, AutoCloseable {

    @Override
    void close();
}
