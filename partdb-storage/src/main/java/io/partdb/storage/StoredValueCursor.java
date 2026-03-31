package io.partdb.storage;

import java.util.Iterator;

interface StoredValueCursor extends Iterator<StoredEntry.Value>, AutoCloseable {

    @Override
    void close();
}
