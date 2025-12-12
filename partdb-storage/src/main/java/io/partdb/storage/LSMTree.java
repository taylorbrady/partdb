package io.partdb.storage;

import io.partdb.common.Entry;
import io.partdb.common.Slice;
import io.partdb.storage.compaction.CompactionStrategy;
import io.partdb.storage.compaction.CompactionTask;
import io.partdb.storage.compaction.Compactor;
import io.partdb.storage.compaction.LeveledCompactionStrategy;
import io.partdb.storage.manifest.Manifest;
import io.partdb.storage.manifest.SSTableInfo;
import io.partdb.storage.memtable.Memtable;
import io.partdb.storage.memtable.SkipListMemtable;
import io.partdb.storage.sstable.BlockCache;
import io.partdb.storage.sstable.S3FifoBlockCache;
import io.partdb.storage.sstable.SSTable;
import io.partdb.storage.sstable.SSTableSetRef;
import io.partdb.storage.sstable.SSTableSetRef.AcquireResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class LSMTree implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(LSMTree.class);
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");
    private static final int MAX_IMMUTABLE_MEMTABLES = 4;
    private static final int MAX_SSTABLE_SET_ACQUIRE_ATTEMPTS = 100;
    private static final long INITIAL_BACKOFF_NANOS = 100;
    private static final long MAX_BACKOFF_NANOS = 10_000;
    private static final Duration SHUTDOWN_DRAIN_TIMEOUT = Duration.ofSeconds(30);

    private final Path dataDirectory;
    private final LSMConfig config;
    private final AtomicReference<Memtable> activeMemtable;
    private final ReentrantLock immutableMemtablesLock;
    private volatile List<Memtable> immutableMemtables;
    private final ReentrantLock rotationLock;
    private final ReentrantLock manifestLock;
    private final Semaphore flushPermits;
    private final AtomicLong sstableIdCounter;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean closed;

    private volatile SSTableSetRef sstableSet;
    private volatile Manifest manifest;
    private final Compactor compactor;
    private final CompactionStrategy compactionStrategy;
    private final ExecutorService compactionExecutor;
    private final AtomicBoolean compacting;
    private final BlockCache blockCache;

    private LSMTree(
        Path dataDirectory,
        LSMConfig config,
        Memtable activeMemtable,
        SSTableSetRef sstableSet,
        Manifest manifest,
        AtomicLong sstableIdCounter,
        Compactor compactor,
        CompactionStrategy compactionStrategy,
        BlockCache blockCache
    ) {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.activeMemtable = new AtomicReference<>(activeMemtable);
        this.immutableMemtablesLock = new ReentrantLock();
        this.immutableMemtables = List.of();
        this.rotationLock = new ReentrantLock();
        this.manifestLock = new ReentrantLock();
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.sstableIdCounter = sstableIdCounter;
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.closed = new AtomicBoolean(false);
        this.sstableSet = sstableSet;
        this.manifest = manifest;
        this.compactor = compactor;
        this.compactionStrategy = compactionStrategy;
        this.compactionExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.compacting = new AtomicBoolean(false);
        this.blockCache = blockCache;
    }

    public static LSMTree open(Path dataDirectory, LSMConfig config) {
        try {
            Files.createDirectories(dataDirectory);

            BlockCache blockCache = config.blockCacheConfig() != null
                ? new S3FifoBlockCache(config.blockCacheConfig().maxSizeInBytes())
                : null;

            Manifest manifest = Manifest.readFrom(dataDirectory);
            List<SSTable> sstables;

            if (manifest.sstables().isEmpty()) {
                sstables = loadSSTables(dataDirectory, blockCache);

                if (!sstables.isEmpty()) {
                    manifest = buildManifestFromSSTables(sstables);
                    manifest.writeTo(dataDirectory);
                }
            } else {
                sstables = loadSSTablesFromManifest(dataDirectory, manifest, blockCache);
            }

            Memtable memtable = new SkipListMemtable();
            AtomicLong idCounter = new AtomicLong(manifest.nextSSTableId());

            CompactionStrategy strategy = new LeveledCompactionStrategy(
                config.leveledCompactionConfig()
            );

            Compactor compactor = new Compactor(
                idCounter::incrementAndGet,
                id -> dataDirectory.resolve(String.format("%06d.sst", id)),
                config.sstableConfig(),
                config.compactionConfig()
            );

            return new LSMTree(
                dataDirectory,
                config,
                memtable,
                SSTableSetRef.of(sstables),
                manifest,
                idCounter,
                compactor,
                strategy,
                blockCache
            );
        } catch (IOException e) {
            throw new LSMException.RecoveryException("Failed to open store", e);
        }
    }

    public void put(Slice key, Slice value, long revision) {
        Mutation mutation = new Mutation.Put(key, value, revision);
        putMutation(mutation);
    }

    public void delete(Slice key, long revision) {
        Mutation mutation = new Mutation.Tombstone(key, revision);
        putMutation(mutation);
    }

    public Optional<Entry> get(Slice key) {
        Optional<Mutation> result = activeMemtable.get().get(key);
        if (result.isPresent()) {
            return toEntry(result.get());
        }

        for (Memtable immutable : immutableMemtables) {
            result = immutable.get(key);
            if (result.isPresent()) {
                return toEntry(result.get());
            }
        }

        SSTableSetRef acquired = acquireSSTableSetRef();
        try {
            for (SSTable sstable : acquired.readers()) {
                result = sstable.get(key);
                if (result.isPresent()) {
                    return toEntry(result.get());
                }
            }
            return Optional.empty();
        } finally {
            acquired.release();
        }
    }

    public Stream<Entry> scan(Slice startKey, Slice endKey) {
        List<Iterator<Mutation>> iterators = new ArrayList<>();

        iterators.add(activeMemtable.get().scan(startKey, endKey));

        for (Memtable immutable : immutableMemtables) {
            iterators.add(immutable.scan(startKey, endKey));
        }

        SSTableSetRef acquired = acquireSSTableSetRef();
        for (SSTable sstable : acquired.readers()) {
            SSTable.Scan scan = sstable.scan();
            if (startKey != null) {
                scan = scan.from(startKey);
            }
            if (endKey != null) {
                scan = scan.until(endKey);
            }
            iterators.add(scan.iterator());
        }

        MergingIterator merged = new MergingIterator(iterators);

        Iterator<Entry> entryIterator = new Iterator<>() {
            private Entry next = advance();

            private Entry advance() {
                while (merged.hasNext()) {
                    if (merged.next() instanceof Mutation.Put p) {
                        return new Entry(p.key(), p.value(), p.revision());
                    }
                }
                return null;
            }

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public Entry next() {
                if (next == null) {
                    throw new NoSuchElementException();
                }
                Entry result = next;
                next = advance();
                return result;
            }
        };

        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(entryIterator, Spliterator.ORDERED | Spliterator.NONNULL),
                false)
            .onClose(acquired::release);
    }

    public void flush() {
        rotationLock.lock();
        try {
            Memtable current = activeMemtable.get();
            if (current.entryCount() == 0) {
                awaitPendingFlushes();
                return;
            }

            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LSMException.FlushException("Interrupted waiting for flush permit", e);
            }

            Memtable newMemtable = new SkipListMemtable();
            immutableMemtablesLock.lock();
            try {
                var updated = new ArrayList<>(immutableMemtables);
                updated.add(current);
                immutableMemtables = List.copyOf(updated);
            } finally {
                immutableMemtablesLock.unlock();
            }
            activeMemtable.set(newMemtable);

            flushExecutor.submit(this::flushPendingMemtables);
        } finally {
            rotationLock.unlock();
        }

        awaitPendingFlushes();
    }

    public byte[] checkpoint() {
        flush();

        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            ByteBuffer header = ByteBuffer.allocate(8);

            manifestLock.lock();
            try {
                header.putLong(manifest.nextSSTableId());
                output.write(header.array());

                ByteBuffer countBuffer = ByteBuffer.allocate(4);
                countBuffer.putInt(manifest.sstables().size());
                output.write(countBuffer.array());

                for (SSTableInfo meta : manifest.sstables()) {
                    ByteBuffer metaBuffer = ByteBuffer.allocate(meta.serializedSize());
                    meta.writeTo(metaBuffer);
                    output.write(metaBuffer.array());
                }
            } finally {
                manifestLock.unlock();
            }

            return output.toByteArray();
        } catch (IOException e) {
            throw new LSMException.CheckpointException("Failed to create checkpoint", e);
        }
    }

    public void restoreFromCheckpoint(byte[] data) {
        rotationLock.lock();
        try {
            awaitPendingFlushes();

            ByteBuffer buffer = ByteBuffer.wrap(data);

            long nextSSTableId = buffer.getLong();
            int sstableCount = buffer.getInt();

            List<SSTableInfo> metadataList = new ArrayList<>(sstableCount);
            for (int i = 0; i < sstableCount; i++) {
                metadataList.add(SSTableInfo.readFrom(buffer));
            }

            Manifest newManifest = new Manifest(nextSSTableId, metadataList);
            List<SSTable> newReaders = loadSSTablesFromManifest(dataDirectory, newManifest, blockCache);

            manifestLock.lock();
            try {
                manifest = newManifest;
                manifest.writeTo(dataDirectory);
                sstableIdCounter.set(nextSSTableId);

                SSTableSetRef oldSet = sstableSet;
                List<SSTable> orphanedReaders = new ArrayList<>(oldSet.readers());
                sstableSet = SSTableSetRef.of(newReaders);
                oldSet.retire(orphanedReaders);
            } finally {
                manifestLock.unlock();
            }

            Memtable newMemtable = new SkipListMemtable();
            immutableMemtablesLock.lock();
            try {
                immutableMemtables = List.of();
            } finally {
                immutableMemtablesLock.unlock();
            }
            activeMemtable.set(newMemtable);
        } catch (Exception e) {
            throw new LSMException.CheckpointException("Failed to restore from checkpoint", e);
        } finally {
            rotationLock.unlock();
        }
    }

    public Manifest manifest() {
        manifestLock.lock();
        try {
            return manifest;
        } finally {
            manifestLock.unlock();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        flush();

        flushExecutor.shutdown();
        compactionExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
            if (!compactionExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flushExecutor.shutdownNow();
            compactionExecutor.shutdownNow();
        }

        SSTableSetRef finalSet = sstableSet;
        List<SSTable> allReaders = new ArrayList<>(finalSet.readers());
        finalSet.retire(allReaders);

        awaitDrain(finalSet, SHUTDOWN_DRAIN_TIMEOUT);
    }

    private void putMutation(Mutation mutation) {
        Memtable memtable = activeMemtable.get();
        memtable.put(mutation);

        if (memtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes()) {
            maybeRotateMemtable(memtable);
        }
    }

    private void maybeRotateMemtable(Memtable fullMemtable) {
        if (!rotationLock.tryLock()) {
            return;
        }
        try {
            Memtable current = activeMemtable.get();
            if (current != fullMemtable) {
                return;
            }
            if (current.sizeInBytes() < config.memtableConfig().maxSizeInBytes()) {
                return;
            }

            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new LSMException.FlushException("Interrupted waiting for flush permit", e);
            }

            Memtable newMemtable = new SkipListMemtable();
            immutableMemtablesLock.lock();
            try {
                var updated = new ArrayList<>(immutableMemtables);
                updated.add(current);
                immutableMemtables = List.copyOf(updated);
            } finally {
                immutableMemtablesLock.unlock();
            }
            activeMemtable.set(newMemtable);

            flushExecutor.submit(this::flushPendingMemtables);
        } finally {
            rotationLock.unlock();
        }
    }

    private Optional<Entry> toEntry(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Tombstone _ -> Optional.empty();
            case Mutation.Put p -> Optional.of(new Entry(p.key(), p.value(), p.revision()));
        };
    }

    private SSTableSetRef acquireSSTableSetRef() {
        long backoffNanos = INITIAL_BACKOFF_NANOS;

        for (int attempt = 0; attempt < MAX_SSTABLE_SET_ACQUIRE_ATTEMPTS; attempt++) {
            SSTableSetRef current = sstableSet;
            AcquireResult result = current.tryAcquire();

            switch (result) {
                case AcquireResult.Success(var acquired) -> {
                    return acquired;
                }
                case AcquireResult.Retired _ -> {
                    if (attempt < 16) {
                        Thread.onSpinWait();
                    } else {
                        LockSupport.parkNanos(backoffNanos);
                        backoffNanos = Math.min(backoffNanos * 2, MAX_BACKOFF_NANOS);
                    }
                }
            }
        }

        throw new LSMException.ConcurrencyException(
            "Failed to acquire SSTableSetRef after " + MAX_SSTABLE_SET_ACQUIRE_ATTEMPTS + " attempts"
        );
    }

    private void awaitPendingFlushes() {
        CompletableFuture<Void> sentinel = new CompletableFuture<>();
        flushExecutor.submit(() -> sentinel.complete(null));
        try {
            sentinel.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new LSMException.FlushException("Flush operation timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LSMException.FlushException("Interrupted waiting for flush", e);
        } catch (ExecutionException e) {
            throw new LSMException.FlushException("Flush failed", e.getCause());
        }
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

    private void flushPendingMemtables() {
        while (true) {
            List<Memtable> current = immutableMemtables;
            if (current.isEmpty()) {
                return;
            }
            Memtable toFlush = current.getFirst();

            try {
                flushMemtableToSSTable(toFlush);
            } finally {
                immutableMemtablesLock.lock();
                try {
                    var updated = new ArrayList<>(immutableMemtables);
                    updated.removeFirst();
                    immutableMemtables = List.copyOf(updated);
                } finally {
                    immutableMemtablesLock.unlock();
                }
                flushPermits.release();
            }
        }
    }

    private void flushMemtableToSSTable(Memtable memtable) {
        try {
            long sstableId = sstableIdCounter.incrementAndGet();
            Path sstablePath = dataDirectory.resolve(String.format("%06d.sst", sstableId));

            long smallestRev;
            long largestRev;
            try (SSTable.Writer writer = SSTable.Writer.create(sstablePath, config.sstableConfig())) {
                Iterator<Mutation> it = memtable.scan(null, null);
                while (it.hasNext()) {
                    writer.add(it.next());
                }
                smallestRev = writer.smallestRevision();
                largestRev = writer.largestRevision();
            }

            SSTable reader = SSTable.open(sstablePath, blockCache);
            SSTableInfo metadata = buildMetadata(reader, sstableId, 0, smallestRev, largestRev);

            manifestLock.lock();
            try {
                List<SSTableInfo> updatedSSTables = new ArrayList<>(manifest.sstables());
                updatedSSTables.addFirst(metadata);
                manifest = new Manifest(sstableIdCounter.get(), updatedSSTables);
                manifest.writeTo(dataDirectory);

                List<SSTable> newReaders = new ArrayList<>();
                newReaders.add(reader);
                newReaders.addAll(sstableSet.readers());

                SSTableSetRef oldSet = sstableSet;
                sstableSet = SSTableSetRef.of(newReaders);
                oldSet.retire(List.of());
            } finally {
                manifestLock.unlock();
            }

            maybeScheduleCompaction();
        } catch (Exception e) {
            throw new LSMException.FlushException("Failed to flush memtable to SSTable", e);
        }
    }

    private void maybeScheduleCompaction() {
        if (closed.get()) {
            return;
        }

        if (!compacting.compareAndSet(false, true)) {
            return;
        }

        compactionExecutor.submit(() -> {
            try {
                runCompaction();
            } finally {
                compacting.set(false);
            }
        });
    }

    private void runCompaction() {
        Manifest current = manifest();
        Optional<CompactionTask> task = compactionStrategy.selectCompaction(current);

        if (task.isEmpty()) {
            return;
        }

        CompactionTask t = task.get();
        List<SSTable> readers = openCompactionSSTables(t.inputs());

        try {
            List<SSTableInfo> outputs = compactor.execute(
                readers,
                t.targetLevel(),
                t.gcTombstones()
            );

            commitCompaction(t.inputs(), outputs);
            deleteOldSSTables(t.inputs());

            maybeScheduleCompaction();
        } catch (Exception e) {
            logger.error("Compaction failed", e);
        } finally {
            readers.forEach(SSTable::close);
        }
    }

    private List<SSTable> openCompactionSSTables(List<SSTableInfo> metadata) {
        List<SSTable> readers = new ArrayList<>();
        for (SSTableInfo meta : metadata) {
            Path path = resolvePath(meta.id());
            readers.add(SSTable.open(path, blockCache));
        }
        return readers;
    }

    private void deleteOldSSTables(List<SSTableInfo> metadata) {
        for (SSTableInfo meta : metadata) {
            Path path = resolvePath(meta.id());
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                logger.warn("Failed to delete old SSTable: {}", path);
            }
        }
    }

    private void commitCompaction(List<SSTableInfo> oldMeta, List<SSTableInfo> newMeta) {
        manifestLock.lock();
        try {
            List<SSTableInfo> updated = new ArrayList<>(manifest.sstables());
            updated.removeAll(oldMeta);
            updated.addAll(newMeta);
            manifest = new Manifest(manifest.nextSSTableId(), updated);
            manifest.writeTo(dataDirectory);

            Set<Long> oldIds = oldMeta.stream()
                .map(SSTableInfo::id)
                .collect(Collectors.toSet());

            List<SSTable> orphanedReaders = new ArrayList<>();
            List<SSTable> retainedReaders = new ArrayList<>();

            for (SSTable reader : sstableSet.readers()) {
                if (oldIds.contains(parseSSTableId(reader.path()))) {
                    orphanedReaders.add(reader);
                } else {
                    retainedReaders.add(reader);
                }
            }

            for (SSTableInfo meta : newMeta) {
                Path path = dataDirectory.resolve(String.format("%06d.sst", meta.id()));
                retainedReaders.add(SSTable.open(path, blockCache));
            }

            SSTableSetRef oldSet = sstableSet;
            sstableSet = SSTableSetRef.of(retainedReaders);
            oldSet.retire(orphanedReaders);
        } finally {
            manifestLock.unlock();
        }
    }

    private Path resolvePath(long id) {
        return dataDirectory.resolve(String.format("%06d.sst", id));
    }

    private static List<SSTable> loadSSTables(Path dataDirectory, BlockCache blockCache) throws IOException {
        List<SSTable> sstables = new ArrayList<>();

        if (!Files.exists(dataDirectory)) {
            return sstables;
        }

        try (Stream<Path> paths = Files.list(dataDirectory)) {
            List<Path> sstablePaths = paths
                .filter(path -> SSTABLE_PATTERN.matcher(path.getFileName().toString()).matches())
                .sorted(Comparator.comparingLong(LSMTree::parseSSTableId).reversed())
                .toList();

            for (Path path : sstablePaths) {
                sstables.add(SSTable.open(path, blockCache));
            }
        }

        return sstables;
    }

    private static long parseSSTableId(Path path) {
        Matcher matcher = SSTABLE_PATTERN.matcher(path.getFileName().toString());
        if (matcher.matches()) {
            return Long.parseLong(matcher.group(1));
        }
        throw new IllegalArgumentException("Invalid SSTable filename: " + path);
    }

    private static Manifest buildManifestFromSSTables(List<SSTable> sstables) throws IOException {
        List<SSTableInfo> metadataList = new ArrayList<>();
        long maxId = 0;

        for (SSTable reader : sstables) {
            long id = parseSSTableId(reader.path());
            maxId = Math.max(maxId, id);

            SSTableInfo metadata = buildMetadata(reader, id, 0);
            metadataList.add(metadata);
        }

        return new Manifest(maxId, metadataList);
    }

    private static List<SSTable> loadSSTablesFromManifest(Path dataDirectory, Manifest manifest, BlockCache blockCache) {
        List<SSTableInfo> sorted = manifest.sstables().stream()
            .sorted(Comparator.comparingLong(SSTableInfo::id).reversed())
            .toList();

        List<SSTable> readers = new ArrayList<>();
        for (SSTableInfo metadata : sorted) {
            Path path = dataDirectory.resolve(String.format("%06d.sst", metadata.id()));
            readers.add(SSTable.open(path, blockCache));
        }

        return readers;
    }

    private static SSTableInfo buildMetadata(SSTable reader, long id, int level) throws IOException {
        return buildMetadata(reader, id, level, reader.smallestRevision(), reader.largestRevision());
    }

    private static SSTableInfo buildMetadata(SSTable reader, long id, int level, long smallestRev, long largestRev) throws IOException {
        byte[] smallestKey = reader.smallestKey().toByteArray();
        byte[] largestKey = reader.largestKey().toByteArray();
        long fileSize = Files.size(reader.path());
        long entryCount = reader.entryCount();

        return new SSTableInfo(id, level, smallestKey, largestKey, smallestRev, largestRev, fileSize, entryCount);
    }
}
