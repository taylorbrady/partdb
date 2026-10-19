package io.partdb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

final class LatestIterator implements Iterator<InternalEntry> {

    private final Iterator<InternalEntry> source;
    private final long revision;
    private final boolean dropTombstones;
    private Slice lastKey;
    private InternalEntry next;

    LatestIterator(Iterator<InternalEntry> source, long revision, boolean dropTombstones) {
        this.source = source;
        this.revision = revision;
        this.dropTombstones = dropTombstones;
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
        InternalEntry current = next;
        advance();
        return current;
    }

    private void advance() {
        while (source.hasNext()) {
            InternalEntry candidate = source.next();
            if (candidate.revision() > revision || candidate.userKey().equals(lastKey)) {
                continue;
            }
            lastKey = candidate.userKey();
            if (dropTombstones && candidate instanceof InternalEntry.Tombstone) {
                continue;
            }
            next = candidate;
            return;
        }
        next = null;
    }
}
