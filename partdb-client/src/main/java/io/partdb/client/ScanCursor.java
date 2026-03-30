package io.partdb.client;

import java.util.Iterator;

public interface ScanCursor extends Iterator<KeyValue>, AutoCloseable {
    @Override
    void close();
}
