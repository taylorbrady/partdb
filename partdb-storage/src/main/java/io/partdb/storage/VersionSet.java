package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class VersionSet {

    private static final Logger log = LoggerFactory.getLogger(VersionSet.class);

    private final ManifestStore manifestStore;
    private final SstableStore sstableStore;
    private final StorageRuntimeStats stats;
    private final AtomicLong nextSstableId;
    private final ReentrantLock changeLock;
    private final Condition versionChanged;
    private final TreeMap<Long, Integer> activeSnapshotCounts;

    private volatile StoreVersion currentVersion;
    private volatile boolean closed;

    VersionSet(StoreVersion initialVersion) {
        this(null, null, initialVersion, null, initialVersion.manifest().nextSSTableId());
    }

    private VersionSet(
        ManifestStore manifestStore,
        SstableStore sstableStore,
        StoreVersion initialVersion,
        StorageRuntimeStats stats,
        long nextSstableId
    ) {
        this.manifestStore = manifestStore;
        this.sstableStore = sstableStore;
        this.stats = stats;
        this.nextSstableId = new AtomicLong(nextSstableId);
        this.currentVersion = Objects.requireNonNull(initialVersion, "initialVersion");
        this.changeLock = new ReentrantLock();
        this.versionChanged = changeLock.newCondition();
        this.activeSnapshotCounts = new TreeMap<>();
    }

    static VersionSet open(
        ManifestStore manifestStore,
        SstableStore sstableStore,
        LoadedStoreVersion initialState,
        StorageRuntimeStats stats
    ) {
        Objects.requireNonNull(manifestStore, "manifestStore");
        Objects.requireNonNull(sstableStore, "sstableStore");
        Objects.requireNonNull(initialState, "initialState");
        Objects.requireNonNull(stats, "stats");

        stats.updateSstables(initialState.manifest());
        return new VersionSet(
            manifestStore,
            sstableStore,
            initialState.toStoreVersion(),
            stats,
            initialState.manifest().nextSSTableId()
        );
    }

    VersionLease acquire(long snapshotRevision) {
        if (snapshotRevision < 0) {
            throw new IllegalArgumentException("snapshotRevision must be non-negative");
        }

        while (true) {
            StoreVersion version = currentVersion;
            if (closed) {
                throw new StorageException.Closed("Version set is closed");
            }

            StoreVersion.Lease lease = version.tryAcquire();
            if (lease != null) {
                recordSnapshot(snapshotRevision);
                return new VersionLease(lease, this, snapshotRevision);
            }

            changeLock.lock();
            try {
                while (!closed && currentVersion == version && version.isRetired()) {
                    try {
                        versionChanged.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new StorageException.IO("Interrupted waiting for store version", e);
                    }
                }
            } finally {
                changeLock.unlock();
            }
        }
    }

    SSTableManifest manifest() {
        return currentVersion.manifest();
    }

    StoreVersion currentVersion() {
        return currentVersion;
    }

    long allocateSstableId() {
        return nextSstableId.incrementAndGet();
    }

    long oldestSnapshotRevision() {
        changeLock.lock();
        try {
            return activeSnapshotCounts.isEmpty() ? Long.MAX_VALUE : activeSnapshotCounts.firstKey();
        } finally {
            changeLock.unlock();
        }
    }

    void apply(VersionEdit edit) {
        Objects.requireNonNull(edit, "edit");
        ensurePersistentState();

        changeLock.lock();
        try {
            ensureOpen();

            ReaderInstall readerInstall = buildReaders(edit);
            VersionRetirement retirement = buildRetirement(edit);
            SSTableManifest updatedManifest = edit.applyTo(currentVersion.manifest(), nextSstableId.get());
            try {
                manifestStore.write(updatedManifest);
                stats.updateSstables(updatedManifest);
                installLocked(new StoreVersion(updatedManifest, readerInstall.readers()), retirement);
            } catch (RuntimeException e) {
                closeReaders(readerInstall.retainedExisting());
                throw e;
            }
        } finally {
            changeLock.unlock();
        }
    }

    void install(StoreVersion nextVersion, VersionRetirement retirement) {
        Objects.requireNonNull(nextVersion, "nextVersion");
        Objects.requireNonNull(retirement, "retirement");

        changeLock.lock();
        try {
            installLocked(nextVersion, retirement);
        } finally {
            changeLock.unlock();
        }
    }

    StoreVersion retireCurrent() {
        changeLock.lock();
        try {
            ensureOpen();
            StoreVersion version = currentVersion;
            version.retire(new VersionRetirement(sstableStore, version.readers(), List.of()));
            return version;
        } finally {
            changeLock.unlock();
        }
    }

    void replaceCurrent(LoadedStoreVersion nextVersion) {
        Objects.requireNonNull(nextVersion, "nextVersion");
        ensurePersistentState();

        changeLock.lock();
        try {
            ensureOpen();
            currentVersion = nextVersion.toStoreVersion();
            nextSstableId.set(nextVersion.manifest().nextSSTableId());
            stats.updateSstables(nextVersion.manifest());
            versionChanged.signalAll();
        } finally {
            changeLock.unlock();
        }
    }

    StoreVersion close() {
        changeLock.lock();
        try {
            closed = true;
            versionChanged.signalAll();
            StoreVersion version = currentVersion;
            version.retire(new VersionRetirement(sstableStore, version.readers(), List.of()));
            return version;
        } finally {
            changeLock.unlock();
        }
    }

    private void recordSnapshot(long snapshotRevision) {
        changeLock.lock();
        try {
            activeSnapshotCounts.merge(snapshotRevision, 1, Integer::sum);
        } finally {
            changeLock.unlock();
        }
    }

    void releaseSnapshot(long snapshotRevision) {
        changeLock.lock();
        try {
            Integer count = activeSnapshotCounts.get(snapshotRevision);
            if (count == null) {
                throw new IllegalStateException("Snapshot revision " + snapshotRevision + " was not acquired");
            }
            if (count == 1) {
                activeSnapshotCounts.remove(snapshotRevision);
            } else {
                activeSnapshotCounts.put(snapshotRevision, count - 1);
            }
            versionChanged.signalAll();
        } finally {
            changeLock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException.Closed("Version set is closed");
        }
    }

    private void ensurePersistentState() {
        if (manifestStore == null || stats == null) {
            throw new IllegalStateException("Persistent version state is not configured");
        }
    }

    private void installLocked(StoreVersion nextVersion, VersionRetirement retirement) {
        ensureOpen();
        StoreVersion previous = currentVersion;
        currentVersion = nextVersion;
        previous.retire(retirement);
        versionChanged.signalAll();
    }

    private void closeReaders(List<SSTableReader> readers) {
        for (SSTableReader reader : readers) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                log.atWarn()
                    .setCause(e)
                    .addKeyValue("sstableId", reader.id())
                    .log("Failed to close SSTable reader");
            }
        }
    }

    private List<SSTableReader> retainReaders(List<SSTableReader> readers) {
        List<SSTableReader> retained = new ArrayList<>(readers.size());
        try {
            for (SSTableReader reader : readers) {
                retained.add(reader.retain());
            }
            return retained;
        } catch (RuntimeException e) {
            closeReaders(retained);
            throw e;
        }
    }

    private ReaderInstall buildReaders(VersionEdit edit) {
        var removedIds = edit.removedIds();
        List<SSTableReader> readers = new ArrayList<>(currentVersion.readers().size() + edit.additions().size());
        List<SSTableReader> retainedExisting = new ArrayList<>(currentVersion.readers().size());

        try {
            if (edit.additionMode() == VersionEdit.AdditionMode.PREPEND) {
                for (InstalledTable addition : edit.additions()) {
                    readers.add(addition.reader());
                }
            }

            for (SSTableReader existingReader : currentVersion.readers()) {
                if (!removedIds.contains(existingReader.id())) {
                    SSTableReader retained = existingReader.retain();
                    retainedExisting.add(retained);
                    readers.add(retained);
                }
            }

            if (edit.additionMode() == VersionEdit.AdditionMode.APPEND) {
                for (InstalledTable addition : edit.additions()) {
                    readers.add(addition.reader());
                }
            }

            return new ReaderInstall(List.copyOf(readers), List.copyOf(retainedExisting));
        } catch (RuntimeException e) {
            closeReaders(retainedExisting);
            throw e;
        }
    }

    private VersionRetirement buildRetirement(VersionEdit edit) {
        if (edit.removals().isEmpty()) {
            return VersionRetirement.none();
        }

        var removedIds = edit.removedIds();
        List<SSTableReader> readersToClose = new ArrayList<>();
        for (SSTableReader existingReader : currentVersion.readers()) {
            if (removedIds.contains(existingReader.id())) {
                readersToClose.add(existingReader);
            }
        }

        return new VersionRetirement(sstableStore, readersToClose, edit.removals());
    }

    private record ReaderInstall(List<SSTableReader> readers, List<SSTableReader> retainedExisting) {}
}
