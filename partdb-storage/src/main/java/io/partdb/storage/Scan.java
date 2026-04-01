package io.partdb.storage;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface Scan extends Iterator<EntryRecord>, Iterable<EntryRecord>, AutoCloseable {

    @Override
    default Iterator<EntryRecord> iterator() {
        return this;
    }

    default Stream<EntryRecord> stream() {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL),
            false
        ).onClose(this::close);
    }

    @Override
    void close();
}
