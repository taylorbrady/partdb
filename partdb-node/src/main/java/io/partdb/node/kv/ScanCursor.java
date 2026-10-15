package io.partdb.node.kv;

import java.util.Iterator;

public interface ScanCursor<T> extends Iterator<T>, AutoCloseable {
    @Override
    void close();
}
