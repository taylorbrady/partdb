package io.partdb.storage;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class CatalogGeneration {

    private final SSTableManifest manifest;
    private final List<SSTableReader> readers;
    private final Map<Long, SSTableReader> readersById;

    private final ReentrantLock lifecycleLock;
    private final Condition drained;

    private int activeLeases;
    private boolean retired;
    private boolean cleanupRunning;
    private boolean cleanupCompleted;
    private List<Runnable> cleanupActions;

    CatalogGeneration(SSTableManifest manifest, List<SSTableReader> readers) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.readers = List.copyOf(Objects.requireNonNull(readers, "readers"));
        this.readersById = indexReaders(this.readers);
        this.lifecycleLock = new ReentrantLock();
        this.drained = lifecycleLock.newCondition();
        this.cleanupActions = List.of();
    }

    CatalogLease tryAcquire() {
        lifecycleLock.lock();
        try {
            if (retired) {
                return null;
            }

            activeLeases++;
            return new CatalogLease(this);
        } finally {
            lifecycleLock.unlock();
        }
    }

    void retire(List<Runnable> cleanupActions) {
        CleanupWork cleanup = null;
        lifecycleLock.lock();
        try {
            if (retired) {
                return;
            }

            retired = true;
            this.cleanupActions = List.copyOf(Objects.requireNonNull(cleanupActions, "cleanupActions"));
            cleanup = maybeStartCleanup();
        } finally {
            lifecycleLock.unlock();
        }

        runCleanup(cleanup);
    }

    boolean awaitDrain(Duration timeout) {
        long remainingNanos = Objects.requireNonNull(timeout, "timeout").toNanos();

        lifecycleLock.lock();
        try {
            while (!cleanupCompleted) {
                if (remainingNanos <= 0) {
                    return false;
                }
                try {
                    remainingNanos = drained.awaitNanos(remainingNanos);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new StorageException.IO("Interrupted waiting for catalog generation to drain", e);
                }
            }
            return true;
        } finally {
            lifecycleLock.unlock();
        }
    }

    void forceDrain() {
        CleanupWork cleanup = null;
        lifecycleLock.lock();
        try {
            retired = true;
            if (cleanupCompleted || cleanupRunning) {
                return;
            }
            cleanupRunning = true;
            cleanup = new CleanupWork(cleanupActions);
        } finally {
            lifecycleLock.unlock();
        }

        runCleanup(cleanup);
    }

    SSTableManifest manifest() {
        return manifest;
    }

    List<SSTableReader> readers() {
        return readers;
    }

    SSTableReader readerFor(long id) {
        SSTableReader reader = readersById.get(id);
        if (reader == null) {
            throw new StorageException.Corruption(
                "Missing SSTable reader for metadata id " + id
            );
        }
        return reader;
    }

    boolean isRetired() {
        lifecycleLock.lock();
        try {
            return retired;
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void releaseLease() {
        CleanupWork cleanup = null;
        lifecycleLock.lock();
        try {
            if (activeLeases <= 0) {
                throw new IllegalStateException("Catalog generation lease over-released");
            }

            activeLeases--;
            cleanup = maybeStartCleanup();
        } finally {
            lifecycleLock.unlock();
        }

        runCleanup(cleanup);
    }

    private CleanupWork maybeStartCleanup() {
        if (!retired || cleanupRunning || cleanupCompleted || activeLeases > 0) {
            return null;
        }

        cleanupRunning = true;
        return new CleanupWork(cleanupActions);
    }

    private void runCleanup(CleanupWork cleanup) {
        if (cleanup == null) {
            return;
        }

        try {
            for (Runnable action : cleanup.actions()) {
                action.run();
            }
        } finally {
            lifecycleLock.lock();
            try {
                cleanupCompleted = true;
                cleanupRunning = false;
                drained.signalAll();
            } finally {
                lifecycleLock.unlock();
            }
        }
    }

    private static Map<Long, SSTableReader> indexReaders(List<SSTableReader> readers) {
        Map<Long, SSTableReader> readersById = new HashMap<>(readers.size());
        for (SSTableReader reader : readers) {
            readersById.put(reader.id(), reader);
        }
        return Map.copyOf(readersById);
    }

    private record CleanupWork(List<Runnable> actions) {}

    static final class CatalogLease implements AutoCloseable {
        private final CatalogGeneration generation;
        private final AtomicBoolean closed;

        private CatalogLease(CatalogGeneration generation) {
            this.generation = generation;
            this.closed = new AtomicBoolean(false);
        }

        SSTableManifest manifest() {
            return generation.manifest();
        }

        List<SSTableReader> readers() {
            return generation.readers();
        }

        SSTableReader readerFor(long id) {
            return generation.readerFor(id);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                generation.releaseLease();
            }
        }
    }
}
