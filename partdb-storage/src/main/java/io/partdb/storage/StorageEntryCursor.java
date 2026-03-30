package io.partdb.storage;

import java.util.Iterator;

interface StorageEntryCursor extends Iterator<StorageEntry>, AutoCloseable {

    @Override
    void close();
}
