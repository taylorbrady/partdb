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
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class TableCatalog implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TableCatalog.class);
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final int MAX_ACQUIRE_ATTEMPTS = 100;
    private static final long INITIAL_BACKOFF_NANOS = 100;
    private static final long MAX_BACKOFF_NANOS = 10_000;
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

    private volatile SSTableManifest manifest;
    private volatile SSTableSetRef currentSet;

    private TableCatalog(
        Path directory,
        LsmConfig config,
        BlockCache cache,
        SSTableManifest manifest,
        SSTableSetRef initialSet,
        StorageRuntimeStats stats
    ) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.config = Objects.requireNonNull(config, "config");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.nextId = new AtomicLong(manifest.nextSSTableId());
        this.stateLock = new ReentrantLock();
        this.manifest = manifest;
        this.currentSet = initialSet;
        this.closed = new AtomicBoolean(false);
        this.stats = Objects.requireNonNull(stats, "stats");
        this.stats.updateSstables(manifest);

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
                manifest,
                SSTableSetRef.of(readers),
                stats
            );
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open TableCatalog", e);
        }
    }

    void flush(Iterator<Mutation> mutations) {
        SSTableMetadata metadata;
        try (SSTableWriter writer = createWriter(0)) {
            while (mutations.hasNext()) {
                writer.add(mutations.next());
            }
            metadata = writer.finish();
        }

        SSTableReader reader = openReader(metadata);
        addFlushed(metadata, reader);
        scheduleCompaction();
    }

    CatalogSnapshot acquire() {
        long backoffNanos = INITIAL_BACKOFF_NANOS;

        for (int attempt = 0; attempt < MAX_ACQUIRE_ATTEMPTS; attempt++) {
            stateLock.lock();
            try {
                SSTableSetRef current = currentSet;
                SSTableManifest manifestSnapshot = manifest;

                switch (current.tryAcquire()) {
                    case SSTableSetRef.AcquireResult.Success(var acquired) -> {
                        return new CatalogSnapshot(acquired, manifestSnapshot);
                    }
                    case SSTableSetRef.AcquireResult.Retired _ -> {
                    }
                }
            } finally {
                stateLock.unlock();
            }

            if (attempt < 16) {
                Thread.onSpinWait();
            } else {
                LockSupport.parkNanos(backoffNanos);
                backoffNanos = Math.min(backoffNanos * 2, MAX_BACKOFF_NANOS);
            }
        }

        throw new IllegalStateException(
            "Failed to acquire SSTable view after " + MAX_ACQUIRE_ATTEMPTS + " attempts"
        );
    }

    SSTableManifest manifest() {
        stateLock.lock();
        try {
            return manifest;
        } finally {
            stateLock.unlock();
        }
    }

    CatalogCheckpoint captureCheckpoint() {
        try (CatalogSnapshot view = acquire()) {
            return CatalogCheckpoint.capture(view);
        }
    }

    void replaceWith(CatalogCheckpoint checkpoint) {
        boolean restored = false;
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
                    SSTableManifest previousManifest = manifest;
                    SSTableManifest newManifest = checkpoint.manifest();
                    SSTableSetRef oldSet = currentSet;
                    oldSet.retire(List.copyOf(oldSet.readers()));
                    awaitDrain(oldSet, SHUTDOWN_DRAIN_TIMEOUT);

                    backupCurrentState(previousManifest);
                    try {
                        CatalogCheckpoint.ActivatedCatalog activated = checkpoint.activate(directory, cache);
                        manifest = activated.manifest();
                        nextId.set(activated.manifest().nextSSTableId());
                        currentSet = SSTableSetRef.of(activated.readers());
                        restored = true;
                    } catch (IOException e) {
                        rollbackRestore(previousManifest);
                        throw new StorageException.IO("Failed to restore checkpoint", e);
                    } catch (RuntimeException e) {
                        rollbackRestore(previousManifest);
                        throw e;
                    }

                    if (restored) {
                        stats.updateSstables(manifest);
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

        SSTableSetRef finalSet = currentSet;
        List<SSTableReader> allReaders = new ArrayList<>(finalSet.readers());
        finalSet.retire(allReaders);
        awaitDrain(finalSet, SHUTDOWN_DRAIN_TIMEOUT);
    }

    private void addFlushed(SSTableMetadata metadata, SSTableReader reader) {
        stateLock.lock();
        try {
            List<SSTableMetadata> updatedMetadata = new ArrayList<>(manifest.sstables());
            updatedMetadata.addFirst(metadata);
            manifest = new SSTableManifest(nextId.get(), updatedMetadata);
            manifest.writeTo(directory);
            stats.updateSstables(manifest);

            List<SSTableReader> newReaders = new ArrayList<>();
            newReaders.add(reader);
            newReaders.addAll(currentSet.readers());

            SSTableSetRef oldSet = currentSet;
            currentSet = SSTableSetRef.of(newReaders);
            oldSet.retire(List.of());
        } finally {
            stateLock.unlock();
        }
    }

    private void applyCompaction(List<SSTableMetadata> removed, List<SSTableMetadata> added) {
        stateLock.lock();
        try {
            List<SSTableMetadata> updated = new ArrayList<>(manifest.sstables());
            updated.removeAll(removed);
            updated.addAll(added);
            manifest = new SSTableManifest(nextId.get(), updated);
            manifest.writeTo(directory);
            stats.updateSstables(manifest);

            Set<Long> removedIds = removed.stream()
                .map(SSTableMetadata::id)
                .collect(Collectors.toSet());

            List<SSTableReader> orphanedReaders = new ArrayList<>();
            List<SSTableReader> retainedReaders = new ArrayList<>();

            for (SSTableReader reader : currentSet.readers()) {
                if (removedIds.contains(reader.id())) {
                    orphanedReaders.add(reader);
                } else {
                    retainedReaders.add(reader);
                }
            }

            for (SSTableMetadata desc : added) {
                retainedReaders.add(openReader(desc));
            }

            SSTableSetRef oldSet = currentSet;
            currentSet = SSTableSetRef.of(retainedReaders);
            oldSet.retire(orphanedReaders);
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
            case CompactionResult.Success(var task, var outputs) -> {
                applyCompaction(task.inputs(), outputs);
                deleteOldSSTables(task.inputs());
            }
            case CompactionResult.Failure(var task, var cause) ->
                log.atError()
                    .addKeyValue("targetLevel", task.targetLevel())
                    .setCause(cause)
                    .log("Compaction failed");
        }
    }

    private void deleteOldSSTables(List<SSTableMetadata> metadata) {
        for (SSTableMetadata table : metadata) {
            try {
                delete(table.id());
            } catch (IOException e) {
                log.atWarn()
                    .addKeyValue("sstableId", table.id())
                    .log("Failed to delete old SSTable");
            }
        }
    }

    private Path resolvePath(long id) {
        return directory.resolve("%06d.sst".formatted(id));
    }

    LsmStats statsSnapshot() {
        return stats.snapshot();
    }

    private void awaitDrain(SSTableSetRef set, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (!set.isDrained()) {
            if (System.nanoTime() > deadlineNanos) {
                for (SSTableReader reader : set.readers()) {
                    reader.close();
                }
                return;
            }
            LockSupport.parkNanos(1_000_000);
        }
    }

    private static List<Long> discoverSSTableIds(Path directory) throws IOException {
        if (!Files.exists(directory)) {
            return List.of();
        }

        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                .map(p -> p.getFileName().toString())
                .map(SSTABLE_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(m -> Long.parseLong(m.group(1)))
                .toList();
        }
    }

    static List<SSTableReader> loadReadersFromManifest(
        Path directory,
        BlockCache cache,
        SSTableManifest manifest
    ) {
        List<SSTableReader> readers = new ArrayList<>(manifest.sstables().size());
        for (SSTableMetadata desc : manifest.sstables()) {
            Path path = directory.resolve("%06d.sst".formatted(desc.id()));
            readers.add(SSTableReader.open(desc.id(), desc.level(), path, cache));
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

    private void rollbackRestore(SSTableManifest previousManifest) throws IOException {
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

        manifest = previousManifest;
        nextId.set(previousManifest.nextSSTableId());
        currentSet = SSTableSetRef.of(loadReadersFromManifest(directory, cache, previousManifest));
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
