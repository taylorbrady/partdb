package io.partdb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

final class LsmStateStore implements StateStore {

    private final LSMTree tree;

    LsmStateStore(LSMTree tree) {
        this.tree = Objects.requireNonNull(tree, "tree must not be null");
    }

    @Override
    public void put(byte[] key, byte[] value, long revision) {
        tree.put(Slice.of(key), Slice.of(value), revision);
    }

    @Override
    public void delete(byte[] key, long revision) {
        tree.delete(Slice.of(key), revision);
    }

    @Override
    public Optional<VersionedEntry> get(byte[] key) {
        return tree.get(Slice.of(key)).map(LsmStateStore::toVersionedEntry);
    }

    @Override
    public StorageCursor scan(byte[] startKeyInclusive, byte[] endKeyExclusive) {
        Slice startKey = startKeyInclusive != null ? Slice.of(startKeyInclusive) : null;
        Slice endKey = endKeyExclusive != null ? Slice.of(endKeyExclusive) : null;
        return new CursorAdapter(tree.scan(startKey, endKey));
    }

    @Override
    public StorageSnapshot snapshot() {
        return new StorageSnapshot(tree.checkpoint());
    }

    @Override
    public void restore(StorageSnapshot snapshot) {
        tree.restoreFromCheckpoint(Objects.requireNonNull(snapshot, "snapshot must not be null").bytes());
    }

    @Override
    public void close() {
        tree.close();
    }

    private static VersionedEntry toVersionedEntry(StorageEntry entry) {
        return new VersionedEntry(
            entry.key().toByteArray(),
            entry.value().toByteArray(),
            entry.revision()
        );
    }

    private static final class CursorAdapter implements StorageCursor {
        private final StorageEntryCursor cursor;

        private CursorAdapter(StorageEntryCursor cursor) {
            this.cursor = cursor;
        }

        @Override
        public boolean hasNext() {
            return cursor.hasNext();
        }

        @Override
        public VersionedEntry next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return toVersionedEntry(cursor.next());
        }

        @Override
        public void close() {
            cursor.close();
        }
    }
}
