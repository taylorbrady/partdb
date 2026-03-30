package io.partdb.client;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

final class GrpcScanCursor implements ScanCursor {
    private static final Object SENTINEL = new Object();

    private final BlockingQueue<Object> queue;
    private final AtomicBoolean closed;
    private KeyValue next;
    private volatile Throwable error;

    GrpcScanCursor() {
        this.queue = new LinkedBlockingQueue<>();
        this.closed = new AtomicBoolean(false);
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
            Object item = queue.take();
            if (item == SENTINEL) {
                if (error != null) {
                    throw new KvClientException.ClusterUnavailable("Scan failed", error);
                }
                return false;
            }
            next = (KeyValue) item;
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
        next = null;
    }
}
