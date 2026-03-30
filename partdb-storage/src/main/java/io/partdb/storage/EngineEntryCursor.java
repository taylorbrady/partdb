package io.partdb.storage;

import java.util.Iterator;

interface EngineEntryCursor extends Iterator<EngineEntry>, AutoCloseable {

    @Override
    void close();
}
