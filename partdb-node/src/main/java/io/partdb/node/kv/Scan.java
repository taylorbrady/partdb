package io.partdb.node.kv;

import java.util.Iterator;

public interface Scan extends Iterator<KeyValue>, AutoCloseable {
    long readRevision();

    @Override
    void close();
}
