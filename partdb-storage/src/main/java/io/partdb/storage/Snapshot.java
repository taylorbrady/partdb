package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class Snapshot implements AutoCloseable {

    private static final int CHARACTERISTICS = Spliterator.ORDERED | Spliterator.DISTINCT | Spliterator.NONNULL | Spliterator.IMMUTABLE;

    private final ReadState state;
    private final AtomicBoolean closed = new AtomicBoolean();

    Snapshot(ReadState state) {
        this.state = state;
    }

    public long revision() {
        return state.revision();
    }

    public Optional<KeyValue> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        ensureOpen();
        return state.find(Slice.of(key)).flatMap(InternalEntry::visible);
    }

    public Stream<KeyValue> scan(KeyRange range) {
        Objects.requireNonNull(range, "range must not be null");
        ensureOpen();
        if (!state.tryRetain()) {
            throw new IllegalStateException("snapshot is closed");
        }
        try {
            Iterator<KeyValue> values = new Visible(state.scan(Bounds.of(range)));
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(values, CHARACTERISTICS), false).onClose(state::release);
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
            throw new IllegalStateException("snapshot is closed");
        }
    }

    private static final class Visible implements Iterator<KeyValue> {
        private final Iterator<InternalEntry> entries;

        private Visible(Iterator<InternalEntry> entries) {
            this.entries = entries;
        }

        @Override
        public boolean hasNext() {
            return entries.hasNext();
        }

        @Override
        public KeyValue next() {
            return switch (entries.next()) {
                case InternalEntry.Value value -> value.toKeyValue();
                case InternalEntry.Tombstone tombstone -> throw new IllegalStateException("scan surfaced tombstone " + tombstone);
            };
        }
    }
}
