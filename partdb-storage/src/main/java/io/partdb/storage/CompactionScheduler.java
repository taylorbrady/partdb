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
    private final CompactionExecutor compactionExecutor;
    private final Supplier<SSTableManifest> manifestSupplier;
    private final StorageRuntimeStats stats;
    private final Consumer<CompactionResult> resultHandler;

    private final CompactionReservations reservations;
    private final ExecutorService executor;
    private final Semaphore concurrencyLimit;
    private final AtomicBoolean closed;
    private final ReentrantReadWriteLock schedulingLock;

    CompactionScheduler(
        LeveledCompactionPlanner planner,
        CompactionExecutor compactionExecutor,
        int maxConcurrent,
        Supplier<SSTableManifest> manifestSupplier,
        StorageRuntimeStats stats,
        Consumer<CompactionResult> resultHandler
    ) {
        this.planner = planner;
        this.compactionExecutor = compactionExecutor;
        this.manifestSupplier = manifestSupplier;
        this.stats = stats;
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
        StorageCompactionEvent event = new StorageCompactionEvent();
        event.inputLevel = task.inputs().stream().mapToInt(SSTableMetadata::level).min().orElse(task.targetLevel());
        event.outputLevel = task.targetLevel();
        event.inputTableCount = task.inputs().size();
        event.inputBytes = task.inputs().stream().mapToLong(SSTableMetadata::fileSizeBytes).sum();
        event.begin();

        stats.compactionStarted();
        long startNanos = System.nanoTime();
        try {
            CompactionResult result = compactionExecutor.compact(task);
            resultHandler.accept(result);
            switch (result) {
                case CompactionResult.Success(_, var outputs) -> {
                    event.outputTableCount = outputs.size();
                    event.outputBytes = outputs.stream().mapToLong(SSTableMetadata::fileSizeBytes).sum();
                    event.success = true;
                    stats.compactionFinished(true, (System.nanoTime() - startNanos) / 1_000_000);
                }
                case CompactionResult.Failure(_, var cause) -> {
                    event.success = false;
                    event.error = cause.getClass().getSimpleName();
                    stats.compactionFinished(false, (System.nanoTime() - startNanos) / 1_000_000);
                }
            }
        } catch (RuntimeException e) {
            event.success = false;
            event.error = e.getClass().getSimpleName();
            stats.compactionFinished(false, (System.nanoTime() - startNanos) / 1_000_000);
            throw e;
        } finally {
            event.commit();
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
