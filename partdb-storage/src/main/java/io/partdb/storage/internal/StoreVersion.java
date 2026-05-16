package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class StoreVersion {

    private final SSTableManifest manifest;
    private final List<SSTableReader> readers;
    private final Map<Long, SSTableReader> readersById;

    private final ReentrantLock lifecycleLock;
    private final Condition drained;

    private int activeLeases;
    private boolean retired;
    private boolean cleanupRunning;
    private boolean cleanupCompleted;
    private VersionRetirement retirement;

    StoreVersion(SSTableManifest manifest, List<SSTableReader> readers) {
        this.manifest = Objects.requireNonNull(manifest, "manifest");
        this.readers = List.copyOf(Objects.requireNonNull(readers, "readers"));
        this.readersById = indexReaders(this.readers);
        this.lifecycleLock = new ReentrantLock();
        this.drained = lifecycleLock.newCondition();
        this.retirement = VersionRetirement.none();
    }

    Lease tryAcquire() {
        lifecycleLock.lock();
        try {
            if (retired) {
                return null;
            }

            activeLeases++;
            return new Lease(this);
        } finally {
            lifecycleLock.unlock();
        }
    }

    void retire(VersionRetirement retirement) {
        CleanupWork cleanup = null;
        lifecycleLock.lock();
        try {
            if (retired) {
                return;
            }

            retired = true;
            this.retirement = Objects.requireNonNull(retirement, "retirement");
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
            cleanup = new CleanupWork(retirement);
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
                throw new IllegalStateException("Store version lease over-released");
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
        return new CleanupWork(retirement);
    }

    private void runCleanup(CleanupWork cleanup) {
        if (cleanup == null) {
            return;
        }

        try {
            if (cleanup.retirement() != null) {
                cleanup.retirement().execute();
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

    private record CleanupWork(VersionRetirement retirement) {}

    static final class Lease implements AutoCloseable {
        private final StoreVersion version;
        private final AtomicBoolean closed;

        private Lease(StoreVersion version) {
            this.version = version;
            this.closed = new AtomicBoolean(false);
        }

        SSTableManifest manifest() {
            return version.manifest();
        }

        List<SSTableReader> readers() {
            return version.readers();
        }

        SSTableReader readerFor(long id) {
            return version.readerFor(id);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                version.releaseLease();
            }
        }
    }
}
