package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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

final class SSTableStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SSTableStore.class);
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final int MAX_ACQUIRE_ATTEMPTS = 100;
    private static final long INITIAL_BACKOFF_NANOS = 100;
    private static final long MAX_BACKOFF_NANOS = 10_000;
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);
    private static final int CHECKPOINT_MAGIC = 0x53544350;
    private static final int CHECKPOINT_VERSION = 1;
    private static final String RESTORE_SUFFIX = ".restore";
    private static final String BACKUP_SUFFIX = ".backup";
    private static final String MANIFEST_FILENAME = "MANIFEST";
    private static final String MANIFEST_TEMP_FILENAME = "MANIFEST.tmp";
    private static final String MANIFEST_BACKUP_FILENAME = MANIFEST_FILENAME + BACKUP_SUFFIX;

    private final Path directory;
    private final LsmConfig config;
    private final BlockCache cache;
    private final AtomicLong nextId;
    private final ReentrantLock stateLock;
    private final CompactionScheduler compactionScheduler;
    private final AtomicBoolean closed;

    private volatile SSTableManifest manifest;
    private volatile SSTableSetRef currentSet;

    private SSTableStore(
        Path directory,
        LsmConfig config,
        BlockCache cache,
        SSTableManifest manifest,
        SSTableSetRef initialSet
    ) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.config = Objects.requireNonNull(config, "config");
        this.cache = Objects.requireNonNull(cache, "cache");
        this.nextId = new AtomicLong(manifest.nextSSTableId());
        this.stateLock = new ReentrantLock();
        this.manifest = manifest;
        this.currentSet = initialSet;
        this.closed = new AtomicBoolean(false);

        LeveledCompactionPlanner planner = new LeveledCompactionPlanner(config);
        CompactionExecutor compactionExecutor = new CompactionExecutor(this, config);
        this.compactionScheduler = new CompactionScheduler(
            planner,
            compactionExecutor,
            config.maxConcurrentCompactions(),
            this::manifest,
            this::handleCompactionResult
        );
    }

    static SSTableStore open(Path directory, LsmConfig config) {
        try {
            Files.createDirectories(directory);

            BlockCache blockCache = config.cacheEnabled()
                ? new S3FifoBlockCache(config.blockCacheMaxBytes())
                : NoOpBlockCache.INSTANCE;

            SSTableManifest manifest = SSTableManifest.readFrom(directory);
            List<Long> discoveredIds = discoverSSTableIds(directory);
            validateManifestState(manifest, discoveredIds);

            List<SSTable> sstables = manifest.sstables().isEmpty()
                ? List.of()
                : loadSSTablesFromManifest(directory, blockCache, manifest);

            return new SSTableStore(
                directory,
                config,
                blockCache,
                manifest,
                SSTableSetRef.of(sstables)
            );
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open SSTableStore", e);
        }
    }

    void flush(Iterator<Mutation> mutations) {
        SSTableMetadata metadata;
        try (SSTable.Builder builder = createBuilder(0)) {
            while (mutations.hasNext()) {
                builder.add(mutations.next());
            }
            metadata = builder.finish();
        }

        SSTable reader = openReader(metadata);
        addFlushed(metadata, reader);
        scheduleCompaction();
    }

    SSTableView acquire() {
        long backoffNanos = INITIAL_BACKOFF_NANOS;

        for (int attempt = 0; attempt < MAX_ACQUIRE_ATTEMPTS; attempt++) {
            stateLock.lock();
            try {
                SSTableSetRef current = currentSet;
                SSTableManifest manifestSnapshot = manifest;

                switch (current.tryAcquire()) {
                    case SSTableSetRef.AcquireResult.Success(var acquired) -> {
                        return new SSTableView(acquired, manifestSnapshot);
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

    byte[] checkpoint() {
        try (SSTableView view = acquire()) {
            return StoreCheckpoint.capture(view).toBytes();
        }
    }

    void restore(byte[] data) {
        StoreCheckpoint checkpoint = StoreCheckpoint.fromBytes(data);
        boolean restored = false;
        try {
            try {
                checkpoint.stage(directory);
                checkpoint.validate(directory);
            } catch (IOException e) {
                throw new StorageException.IO("Failed to stage checkpoint files", e);
            }

            try (CompactionScheduler.Pause ignored = compactionScheduler.pauseAndAwaitQuiescence(SHUTDOWN_DRAIN_TIMEOUT)) {
                stateLock.lock();
                try {
                    SSTableManifest previousManifest = manifest;
                    SSTableManifest newManifest = checkpoint.manifest();
                    SSTableSetRef oldSet = currentSet;
                    oldSet.retire(List.copyOf(oldSet.readers()));
                    awaitDrain(oldSet, SHUTDOWN_DRAIN_TIMEOUT);

                    backupCurrentState(previousManifest);
                    try {
                        ActivatedRestore activated = checkpoint.activate(directory, cache);
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

    SSTable.Builder createBuilder(int level) {
        long id = nextId.incrementAndGet();
        return SSTable.builder(id, level, resolvePath(id), config);
    }

    SSTable openReader(SSTableMetadata metadata) {
        Path path = resolvePath(metadata.id());
        return SSTable.open(metadata.id(), metadata.level(), path, cache);
    }

    SSTable openForCompaction(SSTableMetadata metadata) {
        Path path = resolvePath(metadata.id());
        return SSTable.open(metadata.id(), metadata.level(), path, NoOpBlockCache.INSTANCE);
    }

    void delete(long id) throws IOException {
        Files.deleteIfExists(resolvePath(id));
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        compactionScheduler.close();

        SSTableSetRef finalSet = currentSet;
        List<SSTable> allReaders = new ArrayList<>(finalSet.readers());
        finalSet.retire(allReaders);
        awaitDrain(finalSet, SHUTDOWN_DRAIN_TIMEOUT);
    }

    private void addFlushed(SSTableMetadata metadata, SSTable reader) {
        stateLock.lock();
        try {
            List<SSTableMetadata> updatedMetadata = new ArrayList<>(manifest.sstables());
            updatedMetadata.addFirst(metadata);
            manifest = new SSTableManifest(nextId.get(), updatedMetadata);
            manifest.writeTo(directory);

            List<SSTable> newReaders = new ArrayList<>();
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

            Set<Long> removedIds = removed.stream()
                .map(SSTableMetadata::id)
                .collect(Collectors.toSet());

            List<SSTable> orphanedReaders = new ArrayList<>();
            List<SSTable> retainedReaders = new ArrayList<>();

            for (SSTable reader : currentSet.readers()) {
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
        compactionScheduler.scheduleCompactions();
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

    private void awaitDrain(SSTableSetRef set, Duration timeout) {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();

        while (!set.isDrained()) {
            if (System.nanoTime() > deadlineNanos) {
                for (SSTable reader : set.readers()) {
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

    private static List<SSTable> loadSSTablesFromManifest(
        Path directory,
        BlockCache cache,
        SSTableManifest manifest
    ) {
        List<SSTable> readers = new ArrayList<>(manifest.sstables().size());
        for (SSTableMetadata desc : manifest.sstables()) {
            Path path = directory.resolve("%06d.sst".formatted(desc.id()));
            readers.add(SSTable.open(desc.id(), desc.level(), path, cache));
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
        currentSet = SSTableSetRef.of(loadSSTablesFromManifest(directory, cache, previousManifest));
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

    private static final class StoreCheckpoint {
        private final SSTableManifest manifest;
        private final List<CheckpointSSTable> sstables;

        private StoreCheckpoint(SSTableManifest manifest, List<CheckpointSSTable> sstables) {
            this.manifest = manifest;
            this.sstables = List.copyOf(sstables);
        }

        static StoreCheckpoint capture(SSTableView view) {
            SSTableManifest manifest = view.manifest();
            var readersById = view.all().stream()
                .collect(Collectors.toMap(SSTable::id, reader -> reader));

            List<CheckpointSSTable> sstables = new ArrayList<>(manifest.sstables().size());
            for (SSTableMetadata metadata : manifest.sstables()) {
                SSTable reader = readersById.get(metadata.id());
                if (reader == null) {
                    throw new StorageException.Corruption(
                        "Missing SSTable reader for checkpoint: " + metadata.id()
                    );
                }
                sstables.add(new CheckpointSSTable(metadata.id(), reader.fileBytes()));
            }

            return new StoreCheckpoint(manifest, sstables);
        }

        static StoreCheckpoint fromBytes(byte[] data) {
            try {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                if (buffer.remaining() < Integer.BYTES * 4) {
                    throw new StorageException.Corruption("Checkpoint too small");
                }
                int magic = buffer.getInt();
                if (magic != CHECKPOINT_MAGIC) {
                    throw new StorageException.Corruption(
                        "Invalid checkpoint magic: " + Integer.toHexString(magic)
                    );
                }

                int version = buffer.getInt();
                if (version != CHECKPOINT_VERSION) {
                    throw new StorageException.Corruption("Unsupported checkpoint version: " + version);
                }

                int manifestLength = buffer.getInt();
                if (manifestLength < 0) {
                    throw new StorageException.Corruption("Negative checkpoint manifest length");
                }

                int sstableCount = buffer.getInt();
                if (sstableCount < 0) {
                    throw new StorageException.Corruption("Negative checkpoint SSTable count");
                }
                if (buffer.remaining() < manifestLength) {
                    throw new StorageException.Corruption("Truncated checkpoint manifest");
                }

                byte[] manifestBytes = new byte[manifestLength];
                buffer.get(manifestBytes);
                SSTableManifest manifest = SSTableManifest.fromBytes(manifestBytes);
                if (manifest.sstables().size() != sstableCount) {
                    throw new StorageException.Corruption("Checkpoint SSTable count does not match manifest");
                }

                List<CheckpointSSTable> sstables = new ArrayList<>(sstableCount);
                for (SSTableMetadata metadata : manifest.sstables()) {
                    if (buffer.remaining() < Long.BYTES + Integer.BYTES) {
                        throw new StorageException.Corruption("Truncated checkpoint SSTable header");
                    }
                    long id = buffer.getLong();
                    if (id != metadata.id()) {
                        throw new StorageException.Corruption(
                            "Checkpoint SSTable id does not match manifest: " + id
                        );
                    }

                    int fileLength = buffer.getInt();
                    if (fileLength < 0) {
                        throw new StorageException.Corruption("Negative checkpoint SSTable length");
                    }
                    if (buffer.remaining() < fileLength) {
                        throw new StorageException.Corruption("Truncated checkpoint SSTable payload");
                    }

                    byte[] fileBytes = new byte[fileLength];
                    buffer.get(fileBytes);
                    sstables.add(new CheckpointSSTable(id, fileBytes));
                }

                if (buffer.hasRemaining()) {
                    throw new StorageException.Corruption("Trailing checkpoint data");
                }

                return new StoreCheckpoint(manifest, sstables);
            } catch (BufferUnderflowException | IllegalArgumentException e) {
                throw new StorageException.Corruption("Malformed checkpoint", e);
            }
        }

        SSTableManifest manifest() {
            return manifest;
        }

        byte[] toBytes() {
            byte[] manifestBytes = manifest.toBytes();
            ByteArrayOutputStream output = new ByteArrayOutputStream();

            ByteBuffer header = ByteBuffer.allocate(Integer.BYTES * 4);
            header.putInt(CHECKPOINT_MAGIC);
            header.putInt(CHECKPOINT_VERSION);
            header.putInt(manifestBytes.length);
            header.putInt(sstables.size());
            output.writeBytes(header.array());
            output.writeBytes(manifestBytes);

            for (CheckpointSSTable sstable : sstables) {
                ByteBuffer sstableHeader = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
                sstableHeader.putLong(sstable.id());
                sstableHeader.putInt(sstable.data().length);
                output.writeBytes(sstableHeader.array());
                output.writeBytes(sstable.data());
            }

            return output.toByteArray();
        }

        void stage(Path directory) throws IOException {
            Files.createDirectories(directory);
            for (CheckpointSSTable sstable : sstables) {
                Files.write(
                    stagedPath(directory, sstable.id()),
                    sstable.data(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
                );
            }
        }

        void validate(Path directory) {
            List<SSTable> readers = new ArrayList<>(sstables.size());
            try {
                for (SSTableMetadata metadata : manifest.sstables()) {
                    SSTable reader = SSTable.open(
                        metadata.id(),
                        metadata.level(),
                        stagedPath(directory, metadata.id()),
                        NoOpBlockCache.INSTANCE
                    );
                    readers.add(reader);
                    if (!reader.metadata().equals(metadata)) {
                        throw new StorageException.Corruption(
                            "Checkpoint SSTable metadata does not match file contents: " + metadata.id()
                        );
                    }
                }
            } catch (RuntimeException e) {
                throw new StorageException.Corruption("Checkpoint validation failed", e);
            } finally {
                for (SSTable reader : readers) {
                    reader.close();
                }
            }
        }

        ActivatedRestore activate(Path directory, BlockCache cache) throws IOException {
            commitTo(directory);
            manifest.writeTo(directory);
            return new ActivatedRestore(manifest, loadSSTablesFromManifest(directory, cache, manifest));
        }

        void commitTo(Path directory) throws IOException {
            for (CheckpointSSTable sstable : sstables) {
                Files.move(
                    stagedPath(directory, sstable.id()),
                    directory.resolve("%06d.sst".formatted(sstable.id())),
                    StandardCopyOption.REPLACE_EXISTING
                );
            }
        }

        void cleanup(Path directory) {
            for (CheckpointSSTable sstable : sstables) {
                try {
                    Files.deleteIfExists(stagedPath(directory, sstable.id()));
                } catch (IOException e) {
                    log.atWarn()
                        .setCause(e)
                        .addKeyValue("sstableId", sstable.id())
                        .log("Failed to clean staged checkpoint SSTable");
                }
            }
        }

        private static Path stagedPath(Path directory, long id) {
            return directory.resolve("%06d.sst%s".formatted(id, RESTORE_SUFFIX));
        }
    }

    private record ActivatedRestore(SSTableManifest manifest, List<SSTable> readers) {}

    private record CheckpointSSTable(long id, byte[] data) {}
}
