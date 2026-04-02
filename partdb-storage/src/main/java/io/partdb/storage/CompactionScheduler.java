package io.partdb.storage;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class CompactionScheduler implements AutoCloseable {

    private final LeveledCompactionPlanner planner;
    private final Compactor compactor;
    private final SstableStore sstableStore;
    private final VersionSet versionSet;
    private final StorageStatsCollector stats;
    private final ReservationLedger reservations;
    private final int maxConcurrentCompactions;

    private final ReentrantLock lock;
    private final Condition stateChanged;
    private final Condition idle;
    private final ExecutorService taskExecutor;
    private final Thread coordinatorThread;

    private boolean paused;
    private boolean closed;
    private int activeTasks;
    private long requestedVersion;
    private long processedVersion;

    CompactionScheduler(
        Compactor compactor,
        SstableStore sstableStore,
        VersionSet versionSet,
        LsmConfig config,
        StorageStatsCollector stats
    ) {
        this.planner = new LeveledCompactionPlanner(config);
        this.compactor = Objects.requireNonNull(compactor, "compactor");
        this.sstableStore = Objects.requireNonNull(sstableStore, "sstableStore");
        this.versionSet = Objects.requireNonNull(versionSet, "versionSet");
        this.stats = Objects.requireNonNull(stats, "stats");
        this.reservations = new ReservationLedger();
        this.maxConcurrentCompactions = config.maxConcurrentCompactions();

        this.lock = new ReentrantLock();
        this.stateChanged = lock.newCondition();
        this.idle = lock.newCondition();
        this.taskExecutor = Executors.newThreadPerTaskExecutor(
            Thread.ofVirtual().name("compaction-", 0).factory()
        );
        this.coordinatorThread = Thread.ofVirtual()
            .name("compaction-controller")
            .start(this::runLoop);
    }

    void requestCompaction() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            requestedVersion++;
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    Pause pauseAndAwaitIdle(Duration timeout) {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }

        lock.lock();
        try {
            ensureOpen();
            paused = true;
            stateChanged.signalAll();
            awaitNoActiveTasks(timeout);
            return new Pause(this);
        } catch (RuntimeException e) {
            paused = false;
            stateChanged.signalAll();
            throw e;
        } finally {
            lock.unlock();
        }
    }

    void awaitIdle(Duration timeout) {
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be non-negative");
        }

        lock.lock();
        try {
            long remainingNanos = timeout.toNanos();
            while (!isIdleLocked()) {
                if (remainingNanos <= 0) {
                    throw new StorageException.Timeout("Timed out waiting for compaction idle", null);
                }
                try {
                    remainingNanos = idle.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StorageException.IO("Interrupted waiting for compaction idle", e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            paused = false;
            stateChanged.signalAll();
            idle.signalAll();
        } finally {
            lock.unlock();
        }

        taskExecutor.shutdown();
        try {
            coordinatorThread.join();
            if (!taskExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                taskExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            taskExecutor.shutdownNow();
        }
    }

    private void resume() {
        lock.lock();
        try {
            if (closed || !paused) {
                return;
            }
            paused = false;
            stateChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    private void runLoop() {
        while (true) {
            long targetVersion;
            Set<Long> reservedIds;

            lock.lock();
            try {
                while (!closed && (paused || activeTasks >= maxConcurrentCompactions || requestedVersion == processedVersion)) {
                    try {
                        stateChanged.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                if (closed) {
                    return;
                }
                targetVersion = requestedVersion;
                reservedIds = reservations.reservedSstableIdsSnapshot();
            } finally {
                lock.unlock();
            }

            SSTableManifest plannedManifest = versionSet.manifest();
            List<CompactionTask> plannedTasks = planner.selectCompactions(plannedManifest, reservedIds);
            List<CompactionLaunch> launches = new ArrayList<>();

            lock.lock();
            try {
                if (closed) {
                    return;
                }
                if (paused || activeTasks >= maxConcurrentCompactions) {
                    continue;
                }
                if (!plannedManifest.equals(versionSet.manifest())) {
                    continue;
                }

                for (CompactionTask task : plannedTasks) {
                    if (closed || paused || activeTasks >= maxConcurrentCompactions) {
                        break;
                    }

                    reservations.tryReserve(task).ifPresent(token -> {
                        activeTasks++;
                        launches.add(new CompactionLaunch(task, token));
                    });
                }

                processedVersion = Math.max(processedVersion, targetVersion);
                signalIdleIfNeededLocked();
            } finally {
                lock.unlock();
            }

            for (CompactionLaunch launch : launches) {
                submitLaunch(launch);
            }
        }
    }

    private void submitLaunch(CompactionLaunch launch) {
        try {
            taskExecutor.submit(() -> runCompaction(launch.task(), launch.token()));
        } catch (RejectedExecutionException e) {
            cancelLaunch(launch.token());
        }
    }

    private void cancelLaunch(ReservationToken token) {
        lock.lock();
        try {
            reservations.release(token);
            activeTasks--;
            if (!closed) {
                requestedVersion++;
            }
            stateChanged.signalAll();
            signalIdleIfNeededLocked();
        } finally {
            lock.unlock();
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
            List<SSTableMetadata> outputs = compactor.compact(task);
            publishCompaction(task, outputs);
            event.outputTableCount = outputs.size();
            event.outputBytes = outputs.stream().mapToLong(SSTableMetadata::fileSizeBytes).sum();
            event.success = true;
            stats.compactionFinished(true, (System.nanoTime() - startNanos) / 1_000_000);
        } catch (RuntimeException e) {
            event.success = false;
            event.error = e.getClass().getSimpleName();
            stats.compactionFinished(false, (System.nanoTime() - startNanos) / 1_000_000);
            throw e;
        } finally {
            event.commit();
            lock.lock();
            try {
                reservations.release(token);
                activeTasks--;
                if (!closed) {
                    requestedVersion++;
                }
                stateChanged.signalAll();
                signalIdleIfNeededLocked();
            } finally {
                lock.unlock();
            }
        }
    }

    private void awaitNoActiveTasks(Duration timeout) {
        long remainingNanos = timeout.toNanos();
        while (activeTasks > 0) {
            if (remainingNanos <= 0) {
                throw new IllegalStateException("Timed out waiting for compactions to quiesce");
            }
            try {
                remainingNanos = idle.awaitNanos(remainingNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageException.IO("Interrupted waiting for compaction quiescence", e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException.Closed("Compaction scheduler is closed");
        }
    }

    private boolean isIdleLocked() {
        return activeTasks == 0 && requestedVersion == processedVersion;
    }

    private void publishCompaction(CompactionTask task, List<SSTableMetadata> outputs) {
        List<SSTableReader> outputReaders = sstableStore.openReaders(outputs);
        try {
            List<InstalledTable> installed = new ArrayList<>(outputs.size());
            for (int i = 0; i < outputs.size(); i++) {
                installed.add(new InstalledTable(outputs.get(i), outputReaders.get(i)));
            }
            versionSet.apply(new VersionEdit.Compaction(task.inputs(), installed));
        } catch (RuntimeException e) {
            sstableStore.closeReaders(outputReaders);
            sstableStore.deleteTables(outputs);
            throw e;
        }
    }

    private void signalIdleIfNeededLocked() {
        if (isIdleLocked() || activeTasks == 0) {
            idle.signalAll();
        }
    }

    private record CompactionLaunch(CompactionTask task, ReservationToken token) {}

    private record ReservationToken(
        Set<Long> sstableIds,
        int sourceLevel,
        int targetLevel,
        TableRange keyRange
    ) {

        private ReservationToken {
            Objects.requireNonNull(sstableIds, "sstableIds");
            Objects.requireNonNull(keyRange, "keyRange");
            sstableIds = Set.copyOf(sstableIds);

            if (sstableIds.isEmpty()) {
                throw new IllegalArgumentException("sstableIds cannot be empty");
            }
            if (sourceLevel < 0) {
                throw new IllegalArgumentException("sourceLevel must be non-negative");
            }
            if (targetLevel < 0) {
                throw new IllegalArgumentException("targetLevel must be non-negative");
            }
        }
    }

    private static final class ReservationLedger {
        private final Map<Integer, Set<TableRange>> rangesByLevel = new HashMap<>();
        private final Set<Long> reservedSstableIds = new HashSet<>();

        Set<Long> reservedSstableIdsSnapshot() {
            return Set.copyOf(reservedSstableIds);
        }

        Optional<ReservationToken> tryReserve(CompactionTask task) {
            Set<Long> taskIds = new HashSet<>();
            for (SSTableMetadata input : task.inputs()) {
                taskIds.add(input.id());
            }

            if (!disjoint(reservedSstableIds, taskIds)) {
                return Optional.empty();
            }

            TableRange range = TableRange.from(task.inputs());
            int sourceLevel = sourceLevel(task);
            int targetLevel = task.targetLevel();

            if (hasOverlap(sourceLevel, range) || hasOverlap(targetLevel, range)) {
                return Optional.empty();
            }

            reservedSstableIds.addAll(taskIds);
            addRange(sourceLevel, range);
            if (sourceLevel != targetLevel) {
                addRange(targetLevel, range);
            }

            return Optional.of(new ReservationToken(taskIds, sourceLevel, targetLevel, range));
        }

        void release(ReservationToken token) {
            reservedSstableIds.removeAll(token.sstableIds());
            removeRange(token.sourceLevel(), token.keyRange());
            if (token.sourceLevel() != token.targetLevel()) {
                removeRange(token.targetLevel(), token.keyRange());
            }
        }

        private boolean hasOverlap(int level, TableRange range) {
            Set<TableRange> levelRanges = rangesByLevel.get(level);
            if (levelRanges == null) {
                return false;
            }
            return levelRanges.stream().anyMatch(existing -> existing.overlaps(range));
        }

        private void addRange(int level, TableRange range) {
            rangesByLevel.computeIfAbsent(level, _ -> new HashSet<>()).add(range);
        }

        private void removeRange(int level, TableRange range) {
            Set<TableRange> ranges = rangesByLevel.get(level);
            if (ranges != null) {
                ranges.remove(range);
                if (ranges.isEmpty()) {
                    rangesByLevel.remove(level);
                }
            }
        }

        private static int sourceLevel(CompactionTask task) {
            return task.inputs().stream()
                .mapToInt(SSTableMetadata::level)
                .min()
                .orElse(0);
        }

        private static boolean disjoint(Set<Long> left, Set<Long> right) {
            for (Long id : right) {
                if (left.contains(id)) {
                    return false;
                }
            }
            return true;
        }
    }

    static final class Pause implements AutoCloseable {
        private final CompactionScheduler controller;
        private final AtomicBoolean closed;

        private Pause(CompactionScheduler controller) {
            this.controller = controller;
            this.closed = new AtomicBoolean(false);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                controller.resume();
            }
        }
    }
}
