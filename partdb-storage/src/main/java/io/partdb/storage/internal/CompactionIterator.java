package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

final class CompactionIterator implements Iterator<InternalEntry> {

    private final Iterator<InternalEntry> source;
    private final long oldestSnapshotRevision;
    private final boolean gcTombstones;
    private final ArrayDeque<InternalEntry> ready;

    private InternalEntry pending;

    CompactionIterator(Iterator<InternalEntry> source, long oldestSnapshotRevision, boolean gcTombstones) {
        this.source = source;
        this.oldestSnapshotRevision = oldestSnapshotRevision;
        this.gcTombstones = gcTombstones;
        this.ready = new ArrayDeque<>();
        this.pending = null;
    }

    @Override
    public boolean hasNext() {
        fillReady();
        return !ready.isEmpty();
    }

    @Override
    public InternalEntry next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return ready.removeFirst();
    }

    private void fillReady() {
        while (ready.isEmpty()) {
            InternalEntry first = nextSourceEntry();
            if (first == null) {
                return;
            }

            List<InternalEntry> entriesForKey = new ArrayList<>();
            entriesForKey.add(first);
            Slice userKey = first.userKey();

            while (true) {
                InternalEntry next = nextSourceEntry();
                if (next == null) {
                    break;
                }
                if (!next.userKey().equals(userKey)) {
                    pending = next;
                    break;
                }
                entriesForKey.add(next);
            }

            retainEntries(entriesForKey);
        }
    }

    private InternalEntry nextSourceEntry() {
        if (pending != null) {
            InternalEntry next = pending;
            pending = null;
            return next;
        }
        return source.hasNext() ? source.next() : null;
    }

    private void retainEntries(List<InternalEntry> entriesForKey) {
        if (entriesForKey.isEmpty()) {
            return;
        }

        if (oldestSnapshotRevision == Long.MAX_VALUE) {
            retainWithoutSnapshots(entriesForKey);
            return;
        }

        for (InternalEntry entry : entriesForKey) {
            if (entry.revision() > oldestSnapshotRevision) {
                ready.addLast(entry);
                continue;
            }

            if (gcTombstones && ready.isEmpty() && entry instanceof InternalEntry.Tombstone) {
                return;
            }

            ready.addLast(entry);
            return;
        }
    }

    private void retainWithoutSnapshots(List<InternalEntry> entriesForKey) {
        InternalEntry latest = entriesForKey.getFirst();
        if (gcTombstones && latest instanceof InternalEntry.Tombstone) {
            return;
        }
        ready.addLast(latest);
    }
}
