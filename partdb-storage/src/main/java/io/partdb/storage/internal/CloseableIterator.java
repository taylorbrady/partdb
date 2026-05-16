package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.Iterator;

interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close();
}
