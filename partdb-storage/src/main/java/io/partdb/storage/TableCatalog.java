package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TableCatalog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TableCatalog.class);
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    private static final String BACKUP_SUFFIX = ".backup";
    private static final String MANIFEST_FILENAME = "MANIFEST";
    private static final String MANIFEST_TEMP_FILENAME = "MANIFEST.tmp";
    private static final String MANIFEST_BACKUP_FILENAME = MANIFEST_FILENAME + BACKUP_SUFFIX;

    private final Path directory;
    private final LsmConfig config;
    private final BlockCache cache;
    private final AtomicLong nextId;
    private final ReentrantLock stateLock;
    private final CompactionManager compactionManager;
    private final AtomicBoolean closed;
    private final StorageRuntimeStats stats;
    private final CatalogManager catalogManager;

    private TableCatalog(
        Path directory,
        LsmConfig config,
        BlockCache cache,
        CatalogGeneration initialGeneration,
        StorageRuntimeStats stats
    ) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.config = Objects.requireNonNull(config, "config");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.nextId = new AtomicLong(initialGeneration.manifest().nextSSTableId());
        this.stateLock = new ReentrantLock();
        this.closed = new AtomicBoolean(false);
        this.stats = Objects.requireNonNull(stats, "stats");
        this.catalogManager = new CatalogManager(initialGeneration);
        this.stats.updateSstables(initialGeneration.manifest());

        this.compactionManager = new CompactionManager(
            this,
            config,
            this.stats,
            this::manifest,
            this::handleCompactionResult
        );
    }

    static TableCatalog open(Path directory, LsmConfig config, StorageRuntimeStats stats) {
        try {
            Files.createDirectories(directory);

            BlockCache blockCache = config.cacheEnabled()
                ? new S3FifoBlockCache(config.blockCacheMaxBytes())
                : NoOpBlockCache.INSTANCE;

            SSTableManifest manifest = SSTableManifest.readFrom(directory);
            List<Long> discoveredIds = discoverSSTableIds(directory);
            validateManifestState(manifest, discoveredIds);

            List<SSTableReader> readers = manifest.sstables().isEmpty()
                ? List.of()
                : loadReadersFromManifest(directory, blockCache, manifest);

            return new TableCatalog(
                directory,
                config,
                blockCache,
                new CatalogGeneration(manifest, readers),
                stats
            );
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open TableCatalog", e);
        }
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

    CatalogCheckpoint captureCheckpoint() {
        try (CatalogSnapshot view = acquire()) {
            return CatalogCheckpoint.capture(view);
        }
    }

    void replaceWith(CatalogCheckpoint checkpoint) {
        boolean restored = false;
        boolean backedUpCurrentState = false;

        try {
            try {
                checkpoint.stage(directory);
                checkpoint.validate(directory);
            } catch (IOException e) {
                throw new StorageException.IO("Failed to stage checkpoint files", e);
            }

            try (CompactionScheduler.Pause ignored = compactionManager.pauseAndAwaitQuiescence(SHUTDOWN_DRAIN_TIMEOUT)) {
                stateLock.lock();
                try {
                    SSTableManifest previousManifest = manifest();
                    CatalogGeneration previousGeneration = catalogManager.retireCurrent(
                        closeReadersCleanup(catalogManager.currentGeneration().readers())
                    );

                    if (!previousGeneration.awaitDrain(SHUTDOWN_DRAIN_TIMEOUT)) {
                        try {
                            restoreLiveGeneration(previousManifest);
                        } catch (IOException e) {
                            throw new StorageException.IO("Failed to restore live catalog after timeout", e);
                        }
                        throw new StorageException.Timeout(
                            "Timed out waiting for catalog readers to drain during restore",
                            null
                        );
                    }

                    backupCurrentState(previousManifest);
                    backedUpCurrentState = true;

                    try {
                        CatalogCheckpoint.ActivatedCatalog activated = checkpoint.activate(directory, cache);
                        CatalogGeneration restoredGeneration = new CatalogGeneration(
                            activated.manifest(),
                            activated.readers()
                        );
                        catalogManager.replaceCurrent(restoredGeneration);
                        nextId.set(activated.manifest().nextSSTableId());
                        stats.updateSstables(activated.manifest());
                        restored = true;
                    } catch (IOException e) {
                        rollbackRestore(previousManifest, backedUpCurrentState);
                        throw new StorageException.IO("Failed to restore checkpoint", e);
                    } catch (RuntimeException e) {
                        rollbackRestore(previousManifest, backedUpCurrentState);
                        throw e;
                    }

                    if (restored) {
                        try {
                            cleanupRestoreBackups(previousManifest);
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
            checkpoint.cleanup(directory);
        }

        if (restored) {
            scheduleCompaction();
        }
    }

    SSTableWriter createWriter(int level) {
        long id = nextId.incrementAndGet();
        return SSTableWriter.create(id, level, resolvePath(id), config);
    }

    SSTableReader openReader(SSTableMetadata metadata) {
        Path path = resolvePath(metadata.id());
        return SSTableReader.open(metadata.id(), metadata.level(), path, cache);
    }

    SSTableReader openCompactionReader(SSTableMetadata metadata) {
        Path path = resolvePath(metadata.id());
        return SSTableReader.open(metadata.id(), metadata.level(), path, NoOpBlockCache.INSTANCE);
    }

    void delete(long id) throws IOException {
        Files.deleteIfExists(resolvePath(id));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        compactionManager.close();

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
            SSTableManifest updatedManifest = new SSTableManifest(nextId.get(), updatedMetadata);
            updatedManifest.writeTo(directory);
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
            SSTableManifest updatedManifest = new SSTableManifest(nextId.get(), updated);
            updatedManifest.writeTo(directory);
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
        compactionManager.schedule();
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

    private Path resolvePath(long id) {
        return directory.resolve("%06d.sst".formatted(id));
    }

    LsmStats statsSnapshot() {
        return stats.snapshot();
    }

    private static List<Long> discoverSSTableIds(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return List.of();
        }

        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                .map(path -> path.getFileName().toString())
                .map(SSTABLE_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(matcher -> Long.parseLong(matcher.group(1)))
                .toList();
        }
    }

    static List<SSTableReader> loadReadersFromManifest(
        Path directory,
        BlockCache cache,
        SSTableManifest manifest
    ) {
        List<SSTableReader> readers = new ArrayList<>(manifest.sstables().size());
        for (SSTableMetadata metadata : manifest.sstables()) {
            Path path = directory.resolve("%06d.sst".formatted(metadata.id()));
            readers.add(SSTableReader.open(metadata.id(), metadata.level(), path, cache));
        }

        return readers;
    }

    private static void validateManifestState(SSTableManifest manifest, List<Long> discoveredIds) {
        if (manifest.sstables().isEmpty()) {
            if (!discoveredIds.isEmpty()) {
                throw new StorageException.Corruption(
                    "Found SSTables on disk without manifest entries"
                );
            }
            return;
        }

        Set<Long> manifestIds = manifest.sstables().stream()
            .map(SSTableMetadata::id)
            .collect(Collectors.toCollection(TreeSet::new));
        Set<Long> discoveredIdSet = new TreeSet<>(discoveredIds);

        if (!manifestIds.equals(discoveredIdSet)) {
            Set<Long> missingFromDisk = new TreeSet<>(manifestIds);
            missingFromDisk.removeAll(discoveredIdSet);

            Set<Long> untrackedOnDisk = new TreeSet<>(discoveredIdSet);
            untrackedOnDisk.removeAll(manifestIds);

            throw new StorageException.Corruption(
                "SSTable manifest does not match on-disk SSTables. missingFromDisk=%s, untrackedOnDisk=%s"
                    .formatted(missingFromDisk, untrackedOnDisk)
            );
        }
    }

    private void deleteAllSSTables() throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        try (Stream<Path> paths = Files.list(directory)) {
            for (Path path : paths
                .filter(path -> SSTABLE_PATTERN.matcher(path.getFileName().toString()).matches())
                .toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private void backupCurrentState(SSTableManifest previousManifest) throws IOException {
        for (SSTableMetadata metadata : previousManifest.sstables()) {
            Path livePath = resolvePath(metadata.id());
            if (Files.exists(livePath)) {
                Files.move(
                    livePath,
                    backupPath(metadata.id()),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
                );
            }
        }

        Path manifestPath = directory.resolve(MANIFEST_FILENAME);
        if (Files.exists(manifestPath)) {
            Files.move(
                manifestPath,
                directory.resolve(MANIFEST_BACKUP_FILENAME),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        }

        Files.deleteIfExists(directory.resolve(MANIFEST_TEMP_FILENAME));
    }

    private void rollbackRestore(SSTableManifest previousManifest, boolean backedUpCurrentState) {
        try {
            if (backedUpCurrentState) {
                rollbackRestoreFiles(previousManifest);
            }
            restoreLiveGeneration(previousManifest);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to restore checkpoint", e);
        }
    }

    private void rollbackRestoreFiles(SSTableManifest previousManifest) throws IOException {
        deleteAllSSTables();

        for (SSTableMetadata metadata : previousManifest.sstables()) {
            Path backupPath = backupPath(metadata.id());
            if (Files.exists(backupPath)) {
                Files.move(
                    backupPath,
                    resolvePath(metadata.id()),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
                );
            }
        }

        Files.deleteIfExists(directory.resolve(MANIFEST_FILENAME));
        Files.deleteIfExists(directory.resolve(MANIFEST_TEMP_FILENAME));

        Path manifestBackupPath = directory.resolve(MANIFEST_BACKUP_FILENAME);
        if (Files.exists(manifestBackupPath)) {
            Files.move(
                manifestBackupPath,
                directory.resolve(MANIFEST_FILENAME),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        }
    }

    private void restoreLiveGeneration(SSTableManifest manifest) throws IOException {
        CatalogGeneration liveGeneration = new CatalogGeneration(
            manifest,
            loadReadersFromManifest(directory, cache, manifest)
        );
        nextId.set(manifest.nextSSTableId());
        stats.updateSstables(manifest);
        catalogManager.replaceCurrent(liveGeneration);
    }

    private void cleanupRestoreBackups(SSTableManifest previousManifest) throws IOException {
        for (SSTableMetadata metadata : previousManifest.sstables()) {
            Files.deleteIfExists(backupPath(metadata.id()));
        }
        Files.deleteIfExists(directory.resolve(MANIFEST_BACKUP_FILENAME));
    }

    private Path backupPath(long id) {
        return directory.resolve("%06d.sst%s".formatted(id, BACKUP_SUFFIX));
    }
}
