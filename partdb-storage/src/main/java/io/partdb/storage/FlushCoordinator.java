package io.partdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

final class FlushCoordinator implements AutoCloseable {

    private static final int MAX_IMMUTABLE_MEMTABLES = 4;
    private static final long FLUSH_TIMEOUT_SECONDS = 30;

    private final TableCatalog tableCatalog;
    private final Runnable onImmutableMemtablesChanged;
    private final ReentrantLock immutableMemtablesLock;
    private final Semaphore flushPermits;
    private final ExecutorService flushExecutor;

    private volatile List<ImmutableMemtable> immutableMemtables;

    FlushCoordinator(TableCatalog tableCatalog, Runnable onImmutableMemtablesChanged) {
        this.tableCatalog = tableCatalog;
        this.onImmutableMemtablesChanged = onImmutableMemtablesChanged;
        this.immutableMemtablesLock = new ReentrantLock();
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.immutableMemtables = List.of();
    }

    void enqueue(ImmutableMemtable memtable) {
        try {
            flushPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException.IO("Interrupted waiting for flush permit", e);
        }

        immutableMemtablesLock.lock();
        try {
            var updated = new ArrayList<>(immutableMemtables);
            updated.add(memtable);
            immutableMemtables = List.copyOf(updated);
        } finally {
            immutableMemtablesLock.unlock();
        }

        flushExecutor.submit(this::flushPendingMemtables);
        onImmutableMemtablesChanged.run();
    }

    List<ImmutableMemtable> immutableMemtables() {
        return immutableMemtables;
    }

    void awaitIdle() {
        CompletableFuture<Void> sentinel = new CompletableFuture<>();
        flushExecutor.submit(() -> sentinel.complete(null));
        try {
            sentinel.get(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new StorageException.Timeout("Flush operation timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException.IO("Interrupted waiting for flush", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StorageException storageException) {
                throw storageException;
            }
            throw new StorageException.IO("Flush failed", cause);
        }
    }

    @Override
    public void close() {
        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flushExecutor.shutdownNow();
        }
    }

    private void flushPendingMemtables() {
        while (true) {
            List<ImmutableMemtable> current = immutableMemtables;
            if (current.isEmpty()) {
                return;
            }
            ImmutableMemtable toFlush = current.getFirst();

            try {
                tableCatalog.flush(toFlush.scan(ScanBounds.all()));
            } finally {
                immutableMemtablesLock.lock();
                try {
                    var updated = new ArrayList<>(immutableMemtables);
                    updated.removeFirst();
                    immutableMemtables = List.copyOf(updated);
                } finally {
                    immutableMemtablesLock.unlock();
                }
                flushPermits.release();
                onImmutableMemtablesChanged.run();
            }
        }
    }
}
