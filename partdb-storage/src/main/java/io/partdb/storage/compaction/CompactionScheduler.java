package io.partdb.storage.compaction;

import io.partdb.storage.manifest.Manifest;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

public final class CompactionScheduler implements AutoCloseable {

    private final CompactionStrategy strategy;
    private final Compactor compactor;
    private final Supplier<Manifest> manifestSupplier;
    private final Consumer<CompactionResult> resultHandler;

    private final CompactionReservations reservations;
    private final ExecutorService executor;
    private final Semaphore concurrencyLimit;
    private final AtomicBoolean closed;

    public CompactionScheduler(
        CompactionStrategy strategy,
        Compactor compactor,
        int maxConcurrent,
        Supplier<Manifest> manifestSupplier,
        Consumer<CompactionResult> resultHandler
    ) {
        this.strategy = strategy;
        this.compactor = compactor;
        this.manifestSupplier = manifestSupplier;
        this.resultHandler = resultHandler;

        this.reservations = new CompactionReservations();
        this.executor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("compaction-", 0).factory()
        );
        this.concurrencyLimit = new Semaphore(maxConcurrent);
        this.closed = new AtomicBoolean(false);
    }

    public void scheduleCompactions() {
        if (closed.get()) {
            return;
        }

        Manifest manifest = manifestSupplier.get();
        List<CompactionTask> tasks = strategy.selectCompactions(
            manifest,
            reservations.reservedSSTableIds()
        );

        for (CompactionTask task : tasks) {
            if (!concurrencyLimit.tryAcquire()) {
                break;
            }

            switch (reservations.tryReserve(task)) {
                case ReserveResult.Conflict() -> concurrencyLimit.release();
                case ReserveResult.Success(var token) ->
                    executor.submit(() -> runCompaction(task, token));
            }
        }
    }

    public boolean hasActiveCompactions() {
        return reservations.hasActiveCompactions();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        executor.shutdown();
        try {
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private void runCompaction(CompactionTask task, ReservationToken token) {
        try {
            CompactionResult result = compactor.compact(task);
            resultHandler.accept(result);
        } finally {
            reservations.release(token);
            concurrencyLimit.release();
            reschedule();
        }
    }

    private void reschedule() {
        if (closed.get()) {
            return;
        }
        try {
            executor.submit(this::scheduleCompactions);
        } catch (RejectedExecutionException e) {
        }
    }
}
