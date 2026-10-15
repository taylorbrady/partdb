package io.partdb.storage;

import java.util.Iterator;

public sealed interface Cursor extends Iterator<Entry>, AutoCloseable permits ScanCursor {

    @Override
    void close();
}
