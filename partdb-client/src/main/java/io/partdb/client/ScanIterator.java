package io.partdb.client;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class ScanIterator implements Iterator<KeyValue>, AutoCloseable {

    private static final KeyValue SENTINEL = new KeyValue(null, null, -1);

    private final BlockingQueue<KeyValue> queue;
    private final AtomicBoolean closed;
    private KeyValue next;
    private volatile Throwable error;

    ScanIterator() {
        this.queue = new LinkedBlockingQueue<>();
        this.closed = new AtomicBoolean(false);
        this.next = null;
        this.error = null;
    }

    void addResult(KeyValue kv) {
        if (!closed.get()) {
            queue.offer(kv);
        }
    }

    void complete() {
        queue.offer(SENTINEL);
    }

    void completeWithError(Throwable t) {
        this.error = t;
        queue.offer(SENTINEL);
    }

    @Override
    public boolean hasNext() {
        if (closed.get()) {
            return false;
        }
        if (next != null) {
            return true;
        }
        try {
            next = queue.take();
            if (next == SENTINEL) {
                next = null;
                if (error != null) {
                    throw new KvClientException.ClusterUnavailable("Scan failed", error);
                }
                return false;
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    @Override
    public KeyValue next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        KeyValue result = next;
        next = null;
        return result;
    }

    @Override
    public void close() {
        closed.set(true);
        queue.clear();
    }

    Stream<KeyValue> toStream() {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(this, Spliterator.ORDERED | Spliterator.NONNULL),
                false)
            .onClose(this::close);
    }
}
