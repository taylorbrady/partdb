package io.partdb.common;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close();

    static <T> CloseableIterator<T> wrap(Iterator<T> iterator) {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public T next() {
                return iterator.next();
            }

            @Override
            public void close() {}
        };
    }
}
