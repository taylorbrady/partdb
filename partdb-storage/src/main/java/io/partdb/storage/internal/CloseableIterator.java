package io.partdb.storage.internal;

import java.util.Iterator;

interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close();
}
