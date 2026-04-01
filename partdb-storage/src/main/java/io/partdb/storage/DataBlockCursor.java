package io.partdb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class DataBlockCursor implements Iterator<StoredEntry> {

    private final DataBlockReader reader;
    private final long limit;

    private long offset;
    private byte[] previousKeyBytes;
    private StoredEntry next;

    DataBlockCursor(DataBlockReader reader, long offset, long limit) {
        this.reader = reader;
        this.offset = offset;
        this.limit = limit;
        advance();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public StoredEntry next() {
        if (next == null) {
            throw new NoSuchElementException();
        }

        StoredEntry result = next;
        advance();
        return result;
    }

    void advanceToAtOrAfter(Slice target) {
        while (next != null && next.key().compareTo(target) < 0) {
            next();
        }
    }

    private void advance() {
        if (offset >= limit) {
            next = null;
            return;
        }

        DataBlockReader.DecodedEntry decoded = reader.decodeEntry(offset, previousKeyBytes, limit);
        next = decoded.entry();
        previousKeyBytes = decoded.keyBytes();
        offset = decoded.nextOffset();
    }
}
