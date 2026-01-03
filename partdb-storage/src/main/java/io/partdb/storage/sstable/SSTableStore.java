package io.partdb.storage.sstable;

import io.partdb.storage.Mutation;
import io.partdb.storage.StorageException;
import io.partdb.storage.compaction.CompactionResult;
import io.partdb.storage.compaction.CompactionScheduler;
import io.partdb.storage.compaction.CompactionStrategy;
import io.partdb.storage.compaction.Compactor;
import io.partdb.storage.manifest.Manifest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class SSTableStore implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SSTableStore.class);
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final int MAX_ACQUIRE_ATTEMPTS = 100;
    private static final long INITIAL_BACKOFF_NANOS = 100;
    private static final long MAX_BACKOFF_NANOS = 10_000;
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    private final Path directory;
    private final SSTableConfig config;
    private final BlockCache cache;
    private final AtomicLong nextId;
    private final ReentrantLock stateLock;
    private final CompactionScheduler compactionScheduler;
    private final AtomicBoolean closed;

    private volatile Manifest manifest;
    private volatile SSTableSetRef currentSet;

    private SSTableStore(
        Path directory,
        SSTableConfig config,
        BlockCache cache,
        Manifest manifest,
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

        CompactionStrategy strategy = config.createStrategy();
        Compactor compactor = new Compactor(this, config);
        this.compactionScheduler = new CompactionScheduler(
            strategy,
            compactor,
            config.maxConcurrentCompactions(),
            this::manifest,
            this::handleCompactionResult
        );
    }

    public static SSTableStore open(Path directory, SSTableConfig config) {
        try {
            Files.createDirectories(directory);

            BlockCache blockCache = config.cacheEnabled()
                ? new S3FifoBlockCache(config.blockCacheMaxBytes())
                : NoOpBlockCache.INSTANCE;

            Manifest manifest = Manifest.readFrom(directory);

            List<SSTable> sstables;
            if (manifest.sstables().isEmpty()) {
                sstables = discoverSSTables(directory, blockCache);
                if (!sstables.isEmpty()) {
                    manifest = buildManifestFromSSTables(sstables);
                    manifest.writeTo(directory);
                }
            } else {
                sstables = loadSSTablesFromManifest(directory, blockCache, manifest);
            }

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

    public void flush(Iterator<Mutation> mutations) {
        SSTableDescriptor descriptor;
        try (SSTable.Builder builder = createBuilder(0)) {
            while (mutations.hasNext()) {
                builder.add(mutations.next());
            }
            descriptor = builder.finish();
        }

        SSTable reader = openReader(descriptor);
        addFlushed(descriptor, reader);
        scheduleCompaction();
    }

    public ReadSet acquire() {
        SSTableSetRef acquired = acquireCurrentSet();
        return new ReadSetImpl(acquired, manifest);
    }

    public Manifest manifest() {
        stateLock.lock();
        try {
            return manifest;
        } finally {
            stateLock.unlock();
        }
    }

    public byte[] checkpoint() {
        return manifest().toBytes();
    }

    public void restore(byte[] data) {
        Manifest newManifest = Manifest.fromBytes(data);
        List<SSTable> newReaders = new ArrayList<>();
        for (SSTableDescriptor desc : newManifest.sstables()) {
            newReaders.add(openReader(desc));
        }

        stateLock.lock();
        try {
            manifest = newManifest;
            manifest.writeTo(directory);
            nextId.set(newManifest.nextSSTableId());

            SSTableSetRef oldSet = currentSet;
            List<SSTable> orphanedReaders = new ArrayList<>(oldSet.readers());
            currentSet = SSTableSetRef.of(newReaders);
            oldSet.retire(orphanedReaders);
        } finally {
            stateLock.unlock();
        }
    }

    public SSTable.Builder createBuilder(int level) {
        long id = nextId.incrementAndGet();
        return SSTable.builder(id, level, resolvePath(id), config);
    }

    public SSTable openReader(SSTableDescriptor descriptor) {
        Path path = resolvePath(descriptor.id());
        return SSTable.open(descriptor.id(), descriptor.level(), path, cache);
    }

    public SSTable openForCompaction(SSTableDescriptor descriptor) {
        Path path = resolvePath(descriptor.id());
        return SSTable.open(descriptor.id(), descriptor.level(), path, NoOpBlockCache.INSTANCE);
    }

    public void delete(long id) throws IOException {
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

    private void addFlushed(SSTableDescriptor descriptor, SSTable reader) {
        stateLock.lock();
        try {
            List<SSTableDescriptor> updatedDescriptors = new ArrayList<>(manifest.sstables());
            updatedDescriptors.addFirst(descriptor);
            manifest = new Manifest(nextId.get(), updatedDescriptors);
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

    private void applyCompaction(List<SSTableDescriptor> removed, List<SSTableDescriptor> added) {
        stateLock.lock();
        try {
            List<SSTableDescriptor> updated = new ArrayList<>(manifest.sstables());
            updated.removeAll(removed);
            updated.addAll(added);
            manifest = new Manifest(nextId.get(), updated);
            manifest.writeTo(directory);

            Set<Long> removedIds = removed.stream()
                .map(SSTableDescriptor::id)
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

            for (SSTableDescriptor desc : added) {
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

    private void deleteOldSSTables(List<SSTableDescriptor> descriptors) {
        for (SSTableDescriptor desc : descriptors) {
            try {
                delete(desc.id());
            } catch (IOException e) {
                log.atWarn()
                    .addKeyValue("sstableId", desc.id())
                    .log("Failed to delete old SSTable");
            }
        }
    }

    private Path resolvePath(long id) {
        return directory.resolve("%06d.sst".formatted(id));
    }

    private SSTableSetRef acquireCurrentSet() {
        long backoffNanos = INITIAL_BACKOFF_NANOS;

        for (int attempt = 0; attempt < MAX_ACQUIRE_ATTEMPTS; attempt++) {
            SSTableSetRef current = currentSet;
            SSTableSetRef.AcquireResult result = current.tryAcquire();

            switch (result) {
                case SSTableSetRef.AcquireResult.Success(var acquired) -> {
                    return acquired;
                }
                case SSTableSetRef.AcquireResult.Retired _ -> {
                    if (attempt < 16) {
                        Thread.onSpinWait();
                    } else {
                        LockSupport.parkNanos(backoffNanos);
                        backoffNanos = Math.min(backoffNanos * 2, MAX_BACKOFF_NANOS);
                    }
                }
            }
        }

        throw new IllegalStateException(
            "Failed to acquire SSTableSetRef after " + MAX_ACQUIRE_ATTEMPTS + " attempts"
        );
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

    private static List<SSTable> discoverSSTables(Path directory, BlockCache cache) throws IOException {
        if (!Files.exists(directory)) {
            return List.of();
        }

        try (Stream<Path> paths = Files.list(directory)) {
            List<Long> ids = paths
                .map(p -> p.getFileName().toString())
                .map(SSTABLE_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(m -> Long.parseLong(m.group(1)))
                .sorted(Comparator.reverseOrder())
                .toList();

            List<SSTable> sstables = new ArrayList<>();
            for (long id : ids) {
                sstables.add(SSTable.open(id, 0, directory.resolve("%06d.sst".formatted(id)), cache));
            }
            return sstables;
        }
    }

    private static Manifest buildManifestFromSSTables(List<SSTable> sstables) {
        List<SSTableDescriptor> descriptors = new ArrayList<>();
        long maxId = 0;

        for (SSTable reader : sstables) {
            maxId = Math.max(maxId, reader.id());
            descriptors.add(reader.descriptor());
        }

        return new Manifest(maxId, descriptors);
    }

    private static List<SSTable> loadSSTablesFromManifest(
        Path directory,
        BlockCache cache,
        Manifest manifest
    ) {
        List<SSTableDescriptor> sorted = manifest.sstables().stream()
            .sorted(Comparator.comparingLong(SSTableDescriptor::id).reversed())
            .toList();

        List<SSTable> readers = new ArrayList<>();
        for (SSTableDescriptor desc : sorted) {
            Path path = directory.resolve("%06d.sst".formatted(desc.id()));
            readers.add(SSTable.open(desc.id(), desc.level(), path, cache));
        }

        return readers;
    }

    private static final class ReadSetImpl implements ReadSet {
        private final SSTableSetRef ref;
        private final Manifest manifest;
        private final int maxLevel;

        ReadSetImpl(SSTableSetRef ref, Manifest manifest) {
            this.ref = ref;
            this.manifest = manifest;
            this.maxLevel = manifest.maxLevel();
        }

        @Override
        public List<SSTable> level0() {
            return level(0);
        }

        @Override
        public List<SSTable> level(int level) {
            Set<Long> levelIds = manifest.level(level).stream()
                .map(SSTableDescriptor::id)
                .collect(Collectors.toSet());

            return ref.readers().stream()
                .filter(sst -> levelIds.contains(sst.id()))
                .toList();
        }

        @Override
        public int maxLevel() {
            return maxLevel;
        }

        @Override
        public List<SSTable> all() {
            return ref.readers();
        }

        @Override
        public void close() {
            ref.release();
        }
    }
}
