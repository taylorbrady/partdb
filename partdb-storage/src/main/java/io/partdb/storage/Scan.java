package io.partdb.storage;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface Scan extends Iterable<EntryRecord>, AutoCloseable {

    @Override
    Iterator<EntryRecord> iterator();

    default Stream<EntryRecord> stream() {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(iterator(), Spliterator.ORDERED | Spliterator.NONNULL),
            false
        ).onClose(this::close);
    }

    @Override
    void close();
}
