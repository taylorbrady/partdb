package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

final class SSTableCatalog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SSTableCatalog.class);
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    private final CatalogPersistence persistence;
    private final CatalogRestore restore;
    private final AtomicLong nextId;
    private final ReentrantLock stateLock;
    private final CompactionController compactionController;
    private final AtomicBoolean closed;
    private final StorageRuntimeStats stats;
    private final CatalogManager catalogManager;
    private final AtomicLong appliedThroughRevision;

    private SSTableCatalog(
        LsmConfig config,
        CatalogPersistence persistence,
        CatalogRestore restore,
        LoadedCatalog initialState,
        StorageRuntimeStats stats
    ) {
        Objects.requireNonNull(config, "config");
        this.persistence = Objects.requireNonNull(persistence, "persistence");
        this.restore = Objects.requireNonNull(restore, "restore");
        this.nextId = new AtomicLong(initialState.manifest().nextSSTableId());
        this.stateLock = new ReentrantLock();
        this.closed = new AtomicBoolean(false);
        this.stats = Objects.requireNonNull(stats, "stats");
        this.catalogManager = new CatalogManager(initialState.toGeneration());
        this.appliedThroughRevision = new AtomicLong(initialState.manifest().appliedThroughRevision());
        this.stats.updateSstables(initialState.manifest());

        this.compactionController = new CompactionController(
            this,
            config,
            this.stats,
            this::manifest,
            this::handleCompactionResult
        );
    }

    static SSTableCatalog open(Path directory, LsmConfig config, StorageRuntimeStats stats) {
        BlockCache blockCache = config.cacheEnabled()
            ? new S3FifoBlockCache(config.blockCacheMaxBytes())
            : NoOpBlockCache.INSTANCE;

        CatalogPersistence persistence = new CatalogPersistence(directory, config, blockCache);
        CatalogRestore restore = new CatalogRestore(directory, persistence);
        LoadedCatalog initialState = persistence.openState();

        return new SSTableCatalog(config, persistence, restore, initialState, stats);
    }

    void flush(Iterator<StoredEntry> entries) {
        SSTableMetadata metadata;
        try (SSTableWriter writer = createWriter(0)) {
            while (entries.hasNext()) {
                writer.add(entries.next());
            }
            metadata = writer.finish();
        }

        SSTableReader reader = openReader(metadata);
        try {
            addFlushed(metadata, reader);
            scheduleCompaction();
        } catch (RuntimeException e) {
            closeReaders(List.of(reader));
            deleteSstables(List.of(metadata));
            throw e;
        }
    }

    CatalogSnapshot acquire() {
        return catalogManager.acquire();
    }

    SSTableManifest manifest() {
        return catalogManager.manifest();
    }

    long appliedThroughRevision() {
        return appliedThroughRevision.get();
    }

    void updateAppliedThrough(long revision) {
        appliedThroughRevision.accumulateAndGet(revision, Math::max);
    }

    CatalogCheckpoint captureCheckpoint() {
        try (CatalogSnapshot view = acquire()) {
            return CatalogCheckpoint.capture(view);
        }
    }

    void replaceWith(CatalogCheckpoint checkpoint) {
        boolean restoredStateInstalled = false;
        boolean backedUpCurrentState = false;

        try {
            try {
                restore.stageAndValidate(checkpoint);
            } catch (IOException e) {
                throw new StorageException.IO("Failed to stage checkpoint files", e);
            }

            try (CompactionController.Pause ignored = compactionController.pauseAndAwaitIdle(SHUTDOWN_DRAIN_TIMEOUT)) {
                stateLock.lock();
                try {
                    SSTableManifest previousManifest = manifest();
                    CatalogGeneration previousGeneration = catalogManager.retireCurrent(
                        closeReadersCleanup(catalogManager.currentGeneration().readers())
                    );

                    if (!previousGeneration.awaitDrain(SHUTDOWN_DRAIN_TIMEOUT)) {
                        restoreLiveCatalog(previousManifest);
                        throw new StorageException.Timeout(
                            "Timed out waiting for catalog readers to drain during restore",
                            null
                        );
                    }

                    restore.backupCurrentState(previousManifest);
                    backedUpCurrentState = true;

                    try {
                        installCatalogState(restore.activate(checkpoint));
                        restoredStateInstalled = true;
                    } catch (IOException e) {
                        rollbackRestore(previousManifest, backedUpCurrentState);
                        throw new StorageException.IO("Failed to restore checkpoint", e);
                    } catch (RuntimeException e) {
                        rollbackRestore(previousManifest, backedUpCurrentState);
                        throw e;
                    }

                    if (restoredStateInstalled) {
                        try {
                            restore.cleanupBackups(previousManifest);
                        } catch (IOException e) {
                            log.atWarn()
                                .setCause(e)
                                .log("Failed to clean restore backup files");
                        }
                    }
                } catch (IOException e) {
                    throw new StorageException.IO("Failed to restore checkpoint", e);
                } finally {
                    stateLock.unlock();
                }
            }
        } finally {
            restore.cleanupStaged(checkpoint);
        }

        if (restoredStateInstalled) {
            scheduleCompaction();
        }
    }

    SSTableWriter createWriter(int level) {
        return persistence.createWriter(nextId.incrementAndGet(), level);
    }

    SSTableReader openReader(SSTableMetadata metadata) {
        return persistence.openReader(metadata);
    }

    SSTableReader openCompactionReader(SSTableMetadata metadata) {
        return persistence.openCompactionReader(metadata);
    }

    void delete(long id) throws IOException {
        persistence.delete(id);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        compactionController.close();

        CatalogGeneration finalGeneration = catalogManager.close();
        finalGeneration.retire(closeReadersCleanup(finalGeneration.readers()));
        if (!finalGeneration.awaitDrain(SHUTDOWN_DRAIN_TIMEOUT)) {
            finalGeneration.forceDrain();
        }
    }

    private void addFlushed(SSTableMetadata metadata, SSTableReader reader) {
        stateLock.lock();
        try {
            SSTableManifest currentManifest = manifest();
            CatalogGeneration currentGeneration = catalogManager.currentGeneration();

            List<SSTableMetadata> updatedMetadata = new ArrayList<>(currentManifest.sstables());
            updatedMetadata.addFirst(metadata);
            SSTableManifest updatedManifest = new SSTableManifest(
                nextId.get(),
                appliedThroughRevision.get(),
                updatedMetadata
            );
            persistence.writeManifest(updatedManifest);
            stats.updateSstables(updatedManifest);

            List<SSTableReader> newReaders = new ArrayList<>(currentGeneration.readers().size() + 1);
            newReaders.add(reader);
            newReaders.addAll(currentGeneration.readers());

            catalogManager.install(new CatalogGeneration(updatedManifest, newReaders), List.of());
        } finally {
            stateLock.unlock();
        }
    }

    private void applyCompaction(List<SSTableMetadata> removed, List<SSTableMetadata> added) {
        List<SSTableReader> addedReaders = openReaders(added);

        stateLock.lock();
        try {
            SSTableManifest currentManifest = manifest();
            CatalogGeneration currentGeneration = catalogManager.currentGeneration();

            List<SSTableMetadata> updated = new ArrayList<>(currentManifest.sstables());
            updated.removeAll(removed);
            updated.addAll(added);
            SSTableManifest updatedManifest = new SSTableManifest(
                nextId.get(),
                appliedThroughRevision.get(),
                updated
            );
            persistence.writeManifest(updatedManifest);
            stats.updateSstables(updatedManifest);

            Set<Long> removedIds = removed.stream()
                .map(SSTableMetadata::id)
                .collect(Collectors.toSet());

            List<SSTableReader> orphanedReaders = new ArrayList<>();
            List<SSTableReader> retainedReaders = new ArrayList<>();

            for (SSTableReader reader : currentGeneration.readers()) {
                if (removedIds.contains(reader.id())) {
                    orphanedReaders.add(reader);
                } else {
                    retainedReaders.add(reader);
                }
            }

            retainedReaders.addAll(addedReaders);

            catalogManager.install(
                new CatalogGeneration(updatedManifest, retainedReaders),
                compactionCleanup(orphanedReaders, removed)
            );
        } catch (RuntimeException e) {
            closeReaders(addedReaders);
            deleteSstables(added);
            throw e;
        } finally {
            stateLock.unlock();
        }
    }

    private void scheduleCompaction() {
        if (closed.get()) {
            return;
        }
        compactionController.requestCompaction();
    }

    private void handleCompactionResult(CompactionResult result) {
        switch (result) {
            case CompactionResult.Success(var task, var outputs) -> applyCompaction(task.inputs(), outputs);
            case CompactionResult.Failure(var task, var cause) ->
                log.atError()
                    .addKeyValue("targetLevel", task.targetLevel())
                    .setCause(cause)
                    .log("Compaction failed");
        }
    }

    private List<SSTableReader> openReaders(List<SSTableMetadata> metadata) {
        List<SSTableReader> readers = new ArrayList<>(metadata.size());
        try {
            for (SSTableMetadata table : metadata) {
                readers.add(openReader(table));
            }
            return readers;
        } catch (RuntimeException e) {
            closeReaders(readers);
            throw e;
        }
    }

    private List<Runnable> compactionCleanup(List<SSTableReader> orphanedReaders, List<SSTableMetadata> removed) {
        List<Runnable> cleanup = new ArrayList<>(2);
        if (!orphanedReaders.isEmpty()) {
            cleanup.add(() -> closeReaders(orphanedReaders));
        }
        if (!removed.isEmpty()) {
            cleanup.add(() -> deleteSstables(removed));
        }
        return List.copyOf(cleanup);
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

    private void deleteSstables(List<SSTableMetadata> metadata) {
        for (SSTableMetadata table : metadata) {
            try {
                delete(table.id());
            } catch (IOException e) {
                log.atWarn()
                    .setCause(e)
                    .addKeyValue("sstableId", table.id())
                    .log("Failed to delete SSTable");
            }
        }
    }

    LsmStats statsSnapshot() {
        return stats.snapshot();
    }

    void awaitCompactionIdle(Duration timeout) {
        compactionController.awaitIdle(timeout);
    }

    private void installCatalogState(LoadedCatalog state) {
        catalogManager.replaceCurrent(state.toGeneration());
        nextId.set(state.manifest().nextSSTableId());
        appliedThroughRevision.set(state.manifest().appliedThroughRevision());
        stats.updateSstables(state.manifest());
    }

    private void restoreLiveCatalog(SSTableManifest manifest) {
        installCatalogState(restore.restoreLive(manifest));
    }

    private void rollbackRestore(SSTableManifest previousManifest, boolean backedUpCurrentState) {
        try {
            installCatalogState(restore.rollback(previousManifest, backedUpCurrentState));
        } catch (IOException e) {
            throw new StorageException.IO("Failed to restore checkpoint", e);
        }
    }
}
