package io.partdb.node.kv;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public interface ScanCursor<T> extends Iterator<T>, AutoCloseable {
    default Stream<T> stream() {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL),
            false
        ).onClose(this::close);
    }

    @Override
    void close();
}
