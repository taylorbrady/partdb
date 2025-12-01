package io.partdb.client;

import io.partdb.common.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public final class ScanIterator implements CloseableIterator<KeyValue> {

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
}
