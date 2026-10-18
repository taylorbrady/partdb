package io.partdb.storage;

import java.util.Iterator;

public sealed interface Cursor extends Iterator<KeyValue>, AutoCloseable permits ScanCursor {

    long revision();

    @Override
    void close();
}
