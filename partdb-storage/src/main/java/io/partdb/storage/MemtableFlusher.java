package io.partdb.storage;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

final class MemtableFlusher implements AutoCloseable {

    private static final int MAX_IMMUTABLE_MEMTABLES = 4;
    private static final long FLUSH_TIMEOUT_SECONDS = 30;

    private final SstableStore sstableStore;
    private final VersionSet versionSet;
    private final MemtableSet memtables;
    private final RevisionState revisionState;
    private final CompactionScheduler compactionScheduler;
    private final StorageStatsCollector stats;
    private final Semaphore flushPermits;
    private final ExecutorService flushExecutor;
    private final AtomicReference<StorageException.IO> backgroundFailure;

    MemtableFlusher(
        SstableStore sstableStore,
        VersionSet versionSet,
        MemtableSet memtables,
        RevisionState revisionState,
        CompactionScheduler compactionScheduler,
        StorageStatsCollector stats
    ) {
        this.sstableStore = sstableStore;
        this.versionSet = versionSet;
        this.memtables = memtables;
        this.revisionState = revisionState;
        this.compactionScheduler = compactionScheduler;
        this.stats = stats;
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.backgroundFailure = new AtomicReference<>();
    }

    void reserveSlot() {
        throwIfFailed();
        try {
            flushPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException.IO("Interrupted waiting for flush permit", e);
        }
    }

    void releaseReservedSlot() {
        flushPermits.release();
    }

    void schedule(ImmutableMemtable memtable) {
        throwIfFailed();
        try {
            flushExecutor.submit(() -> flushPendingMemtables(memtable));
        } catch (RuntimeException e) {
            StorageException.IO failure = new StorageException.IO("Failed to schedule memtable flush", e);
            backgroundFailure.compareAndSet(null, failure);
            throw failure;
        }
        refreshMemtableStats();
    }

    void awaitIdle() {
        CompletableFuture<Void> sentinel = new CompletableFuture<>();
        flushExecutor.submit(() -> sentinel.complete(null));
        try {
            sentinel.get(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            throwIfFailed();
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

    void throwIfFailed() {
        StorageException.IO failure = backgroundFailure.get();
        if (failure != null) {
            throw failure;
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

    private void flushPendingMemtables(ImmutableMemtable expectedHead) {
        while (true) {
            throwIfFailed();

            var current = memtables.immutableMemtables();
            if (current.isEmpty()) {
                return;
            }

            ImmutableMemtable toFlush = current.getFirst();
            if (expectedHead != null && toFlush != expectedHead) {
                return;
            }

            try {
                flushMemtable(toFlush);
                memtables.retire(toFlush);
                flushPermits.release();
                refreshMemtableStats();
                compactionScheduler.requestCompaction();
            } catch (RuntimeException e) {
                backgroundFailure.compareAndSet(null, new StorageException.IO("Memtable flush failed", e));
                throwIfFailed();
            }

            expectedHead = null;
        }
    }

    private void flushMemtable(ImmutableMemtable memtable) {
        SSTableMetadata metadata;
        try (SSTableWriter writer = sstableStore.createWriter(versionSet.allocateSstableId(), 0)) {
            var entries = memtable.scan(ScanBounds.all());
            while (entries.hasNext()) {
                writer.add(entries.next());
            }
            metadata = writer.finish();
        }

        SSTableReader reader = sstableStore.openReader(metadata);
        try {
            versionSet.apply(new VersionEdit.Flush(new InstalledTable(metadata, reader), memtable.maxRevision()));
            revisionState.recordDurable(memtable.maxRevision());
        } catch (RuntimeException e) {
            sstableStore.closeReaders(java.util.List.of(reader));
            sstableStore.deleteTables(java.util.List.of(metadata));
            throw e;
        }
    }

    private void refreshMemtableStats() {
        stats.updateMemtables(memtables.activeSizeInBytes(), memtables.immutableCount());
    }
}
