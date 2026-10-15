package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

final class StoreSnapshot implements Snapshot {

    private final ReadState state;
    private final AtomicBoolean closed = new AtomicBoolean();

    StoreSnapshot(ReadState state) {
        this.state = state;
    }

    @Override
    public long index() {
        return state.index();
    }

    @Override
    public Optional<Entry> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        ensureOpen();
        return state.find(Slice.of(key)).flatMap(InternalEntry::visible);
    }

    @Override
    public Cursor scan(KeyRange range) {
        Objects.requireNonNull(range, "range must not be null");
        ensureOpen();
        if (!state.tryRetain()) {
            throw new IllegalStateException("Snapshot is closed");
        }
        try {
            return new ScanCursor(state, state.scan(Bounds.of(range)));
        } catch (RuntimeException e) {
            state.release();
            throw e;
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            state.release();
        }
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Snapshot is closed");
        }
    }
}
