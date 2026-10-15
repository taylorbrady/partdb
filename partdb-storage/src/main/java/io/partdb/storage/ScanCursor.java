package io.partdb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class ScanCursor implements Cursor {

    private final ReadState state;
    private final Iterator<InternalEntry> entries;
    private boolean closed;

    ScanCursor(ReadState state, Iterator<InternalEntry> entries) {
        this.state = state;
        this.entries = entries;
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Cursor is closed");
        }
        return entries.hasNext();
    }

    @Override
    public Entry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return switch (entries.next()) {
            case InternalEntry.Value value -> value.toEntry();
            case InternalEntry.Tombstone tombstone -> throw new IllegalStateException("Scan surfaced tombstone " + tombstone);
        };
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            state.release();
        }
    }
}
