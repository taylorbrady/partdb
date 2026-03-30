package io.partdb.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;

final class LsmStateStore implements StateStore {

    private final LsmEngine engine;

    LsmStateStore(LsmEngine engine) {
        this.engine = Objects.requireNonNull(engine, "engine must not be null");
    }

    @Override
    public void put(byte[] key, byte[] value, long revision) {
        engine.put(Slice.of(key), Slice.of(value), revision);
    }

    @Override
    public void delete(byte[] key, long revision) {
        engine.delete(Slice.of(key), revision);
    }

    @Override
    public Optional<VersionedEntry> get(byte[] key) {
        return engine.get(Slice.of(key)).map(LsmStateStore::toVersionedEntry);
    }

    @Override
    public StorageCursor scan(byte[] startKeyInclusive, byte[] endKeyExclusive) {
        Slice startKey = startKeyInclusive != null ? Slice.of(startKeyInclusive) : null;
        Slice endKey = endKeyExclusive != null ? Slice.of(endKeyExclusive) : null;
        return new CursorAdapter(engine.scan(startKey, endKey));
    }

    @Override
    public StorageSnapshot snapshot() {
        return new StorageSnapshot(engine.checkpoint());
    }

    @Override
    public void restore(StorageSnapshot snapshot) {
        engine.restoreFromCheckpoint(Objects.requireNonNull(snapshot, "snapshot must not be null").bytes());
    }

    @Override
    public void close() {
        engine.close();
    }

    private static VersionedEntry toVersionedEntry(EngineEntry entry) {
        return new VersionedEntry(
            entry.key().toByteArray(),
            entry.value().toByteArray(),
            entry.revision()
        );
    }

    private static final class CursorAdapter implements StorageCursor {
        private final EngineEntryCursor cursor;

        private CursorAdapter(EngineEntryCursor cursor) {
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
