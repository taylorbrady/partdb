package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class DataBlockCursor implements Iterator<InternalEntry> {

    private final DataBlockReader reader;
    private final long limit;

    private long offset;
    private byte[] previousKeyBytes;
    private InternalEntry next;

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
    public InternalEntry next() {
        if (next == null) {
            throw new NoSuchElementException();
        }

        InternalEntry result = next;
        advance();
        return result;
    }

    void advanceToAtOrAfter(Slice target) {
        while (next != null && next.userKey().compareTo(target) < 0) {
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
