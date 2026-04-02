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
    private final StorageRuntimeStats stats;
    private final AtomicLong nextSstableId;
    private final ReentrantLock changeLock;
    private final Condition versionChanged;
    private final TreeMap<Long, Integer> activeSnapshotCounts;

    private volatile StoreVersion currentVersion;
    private volatile boolean closed;

    VersionSet(StoreVersion initialVersion) {
        this(null, initialVersion, null, initialVersion.manifest().nextSSTableId());
    }

    private VersionSet(
        ManifestStore manifestStore,
        StoreVersion initialVersion,
        StorageRuntimeStats stats,
        long nextSstableId
    ) {
        this.manifestStore = manifestStore;
        this.stats = stats;
        this.nextSstableId = new AtomicLong(nextSstableId);
        this.currentVersion = Objects.requireNonNull(initialVersion, "initialVersion");
        this.changeLock = new ReentrantLock();
        this.versionChanged = changeLock.newCondition();
        this.activeSnapshotCounts = new TreeMap<>();
    }

    static VersionSet open(ManifestStore manifestStore, LoadedStoreVersion initialState, StorageRuntimeStats stats) {
        Objects.requireNonNull(manifestStore, "manifestStore");
        Objects.requireNonNull(initialState, "initialState");
        Objects.requireNonNull(stats, "stats");

        stats.updateSstables(initialState.manifest());
        return new VersionSet(
            manifestStore,
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
                return new VersionLease(lease, () -> releaseSnapshot(snapshotRevision));
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

    void addFlushed(SSTableMetadata metadata, SSTableReader reader, long appliedThroughRevision) {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(reader, "reader");
        ensurePersistentState();

        changeLock.lock();
        try {
            ensureOpen();

            SSTableManifest currentManifest = currentVersion.manifest();
            List<SSTableMetadata> updatedMetadata = new ArrayList<>(currentManifest.sstables());
            updatedMetadata.addFirst(metadata);
            SSTableManifest updatedManifest = new SSTableManifest(
                nextSstableId.get(),
                appliedThroughRevision,
                updatedMetadata
            );
            manifestStore.write(updatedManifest);
            stats.updateSstables(updatedManifest);

            List<SSTableReader> readers = new ArrayList<>(currentVersion.readers().size() + 1);
            readers.add(reader);
            readers.addAll(retainReaders(currentVersion.readers()));

            installLocked(new StoreVersion(updatedManifest, readers), List.of());
        } finally {
            changeLock.unlock();
        }
    }

    void applyCompaction(
        List<SSTableMetadata> removed,
        List<SSTableMetadata> added,
        List<SSTableReader> addedReaders,
        long appliedThroughRevision,
        List<Runnable> cleanupActions
    ) {
        Objects.requireNonNull(removed, "removed");
        Objects.requireNonNull(added, "added");
        Objects.requireNonNull(addedReaders, "addedReaders");
        Objects.requireNonNull(cleanupActions, "cleanupActions");
        ensurePersistentState();

        changeLock.lock();
        try {
            ensureOpen();

            SSTableManifest currentManifest = currentVersion.manifest();
            List<SSTableMetadata> updated = new ArrayList<>(currentManifest.sstables());
            updated.removeAll(removed);
            updated.addAll(added);
            SSTableManifest updatedManifest = new SSTableManifest(
                nextSstableId.get(),
                appliedThroughRevision,
                updated
            );
            manifestStore.write(updatedManifest);
            stats.updateSstables(updatedManifest);

            var removedIds = removed.stream().map(SSTableMetadata::id).collect(java.util.stream.Collectors.toSet());
            List<SSTableReader> orphanedReaders = new ArrayList<>();
            List<SSTableReader> retainedReaders = new ArrayList<>();

            for (SSTableReader existingReader : currentVersion.readers()) {
                if (removedIds.contains(existingReader.id())) {
                    orphanedReaders.add(existingReader);
                } else {
                    retainedReaders.add(existingReader.retain());
                }
            }

            retainedReaders.addAll(addedReaders);

            List<Runnable> allCleanup = new ArrayList<>(cleanupActions);
            if (!orphanedReaders.isEmpty()) {
                allCleanup.add(() -> closeReaders(orphanedReaders));
            }

            installLocked(new StoreVersion(updatedManifest, retainedReaders), List.copyOf(allCleanup));
        } finally {
            changeLock.unlock();
        }
    }

    void install(StoreVersion nextVersion, List<Runnable> cleanupActions) {
        Objects.requireNonNull(nextVersion, "nextVersion");
        Objects.requireNonNull(cleanupActions, "cleanupActions");

        changeLock.lock();
        try {
            installLocked(nextVersion, cleanupActions);
        } finally {
            changeLock.unlock();
        }
    }

    StoreVersion retireCurrent() {
        changeLock.lock();
        try {
            ensureOpen();
            StoreVersion version = currentVersion;
            version.retire(closeReadersCleanup(version.readers()));
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
            version.retire(closeReadersCleanup(version.readers()));
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

    private void releaseSnapshot(long snapshotRevision) {
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

    private void installLocked(StoreVersion nextVersion, List<Runnable> cleanupActions) {
        ensureOpen();
        StoreVersion previous = currentVersion;
        currentVersion = nextVersion;
        previous.retire(cleanupActions);
        versionChanged.signalAll();
    }

    private List<Runnable> closeReadersCleanup(List<SSTableReader> readers) {
        if (readers.isEmpty()) {
            return List.of();
        }
        return List.of(() -> closeReaders(readers));
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
}
