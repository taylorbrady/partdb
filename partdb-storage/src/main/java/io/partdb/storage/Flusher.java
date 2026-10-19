package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

final class Flusher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Flusher.class);
    private static final int MAX_PENDING = 4;

    private final Consumer<Memtable> flush;
    private final Semaphore slots = new Semaphore(MAX_PENDING);
    private final ExecutorService executor = Executors.newSingleThreadExecutor(
        Thread.ofPlatform().name("partdb-flush").daemon(true).factory()
    );
    private final AtomicReference<StoreException> failure = new AtomicReference<>();

    Flusher(Consumer<Memtable> flush) {
        this.flush = flush;
    }

    void reserve() {
        throwIfFailed();
        try {
            slots.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException.IO("Interrupted waiting for a memtable flush slot", e);
        }
    }

    CompletableFuture<Void> submit(Memtable memtable) {
        var done = new CompletableFuture<Void>();
        executor.execute(() -> {
            try {
                StoreException previous = failure.get();
                if (previous != null) {
                    done.completeExceptionally(previous);
                    return;
                }
                flush.accept(memtable);
                done.complete(null);
            } catch (RuntimeException e) {
                log.atError().setCause(e).log("Memtable flush failed");
                failure.compareAndSet(null, e instanceof StoreException storeException
                    ? storeException
                    : new StoreException.IO("Memtable flush failed", e));
                done.completeExceptionally(failure.get());
            } finally {
                slots.release();
            }
        });
        return done;
    }

    void awaitIdle() {
        try {
            executor.submit(() -> {}).get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StoreException.IO("Interrupted waiting for memtable flush", e);
        } catch (ExecutionException e) {
            throw new StoreException.IO("Memtable flush failed", e.getCause());
        }
        throwIfFailed();
    }

    void throwIfFailed() {
        StoreException cause = failure.get();
        if (cause != null) {
            throw new StoreException.IO("Memtable flush failed", cause);
        }
    }

    @Override
    public void close() {
        executor.close();
    }
}
