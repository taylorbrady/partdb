package io.partdb.storage;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Supplier;

final class CompactionScheduler implements AutoCloseable {

    private final LeveledCompactionPlanner planner;
    private final Compactor compactor;
    private final Supplier<SSTableManifest> manifestSupplier;
    private final Consumer<CompactionResult> resultHandler;

    private final CompactionReservations reservations;
    private final ExecutorService executor;
    private final Semaphore concurrencyLimit;
    private final AtomicBoolean closed;
    private final ReentrantReadWriteLock schedulingLock;

    CompactionScheduler(
        LeveledCompactionPlanner planner,
        Compactor compactor,
        int maxConcurrent,
        Supplier<SSTableManifest> manifestSupplier,
        Consumer<CompactionResult> resultHandler
    ) {
        this.planner = planner;
        this.compactor = compactor;
        this.manifestSupplier = manifestSupplier;
        this.resultHandler = resultHandler;

        this.reservations = new CompactionReservations();
        this.executor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("compaction-", 0).factory()
        );
        this.concurrencyLimit = new Semaphore(maxConcurrent);
        this.closed = new AtomicBoolean(false);
        this.schedulingLock = new ReentrantReadWriteLock();
    }

    void scheduleCompactions() {
        Lock readLock = schedulingLock.readLock();
        readLock.lock();
        try {
            if (closed.get()) {
                return;
            }

            SSTableManifest manifest = manifestSupplier.get();
            List<CompactionTask> tasks = planner.selectCompactions(
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
        } finally {
            readLock.unlock();
        }
    }

    boolean hasActiveCompactions() {
        return reservations.hasActiveCompactions();
    }

    Pause pauseAndAwaitQuiescence(Duration timeout) {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }

        Lock writeLock = schedulingLock.writeLock();
        writeLock.lock();
        boolean success = false;
        try {
            awaitQuiescence(timeout);
            success = true;
            return new Pause(writeLock);
        } finally {
            if (!success) {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void close() {
        Lock writeLock = schedulingLock.writeLock();
        writeLock.lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
        } finally {
            writeLock.unlock();
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
        Lock readLock = schedulingLock.readLock();
        readLock.lock();
        try {
            if (closed.get()) {
                return;
            }
            try {
                executor.submit(this::scheduleCompactions);
            } catch (RejectedExecutionException e) {
            }
        } finally {
            readLock.unlock();
        }
    }

    private void awaitQuiescence(Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (reservations.hasActiveCompactions()) {
            if (System.nanoTime() >= deadlineNanos) {
                throw new IllegalStateException("Timed out waiting for compactions to quiesce");
            }
            LockSupport.parkNanos(1_000_000);
        }
    }

    static final class Pause implements AutoCloseable {
        private final Lock writeLock;
        private boolean closed;

        private Pause(Lock writeLock) {
            this.writeLock = writeLock;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            writeLock.unlock();
        }
    }
}
