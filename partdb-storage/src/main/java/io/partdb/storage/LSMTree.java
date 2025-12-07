package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.KeyValue;
import io.partdb.storage.compaction.CompactionStrategy;
import io.partdb.storage.compaction.CompactionTask;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.compaction.LeveledCompactionStrategy;
import io.partdb.storage.compaction.Manifest;
import io.partdb.storage.compaction.ManifestFile;
import io.partdb.storage.compaction.SSTableMetadata;
import io.partdb.storage.SSTableSet.AcquireResult;
import io.partdb.storage.memtable.Memtable;
import io.partdb.storage.memtable.SkipListMemtable;
import io.partdb.storage.sstable.SSTableReader;
import io.partdb.storage.sstable.SSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.StampedLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private final Deque<Memtable> immutableMemtables;
    private final StampedLock memtableLock;
    private final ReentrantLock rotationLock;
    private final ReentrantLock manifestLock;
    private final Semaphore flushPermits;
    private final AtomicLong sstableIdCounter;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean closed;

    private volatile SSTableSet sstableSet;
    private volatile Manifest manifest;
    private Compactor compactor;

    private LSMTree(
        Path dataDirectory,
        LSMConfig config,
        Memtable activeMemtable,
        SSTableSet sstableSet,
        Manifest manifest,
        AtomicLong sstableIdCounter
    ) {
        this.dataDirectory = dataDirectory;
        this.config = config;
        this.activeMemtable = new AtomicReference<>(activeMemtable);
        this.immutableMemtables = new ArrayDeque<>();
        this.memtableLock = new StampedLock();
        this.rotationLock = new ReentrantLock();
        this.manifestLock = new ReentrantLock();
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.sstableIdCounter = sstableIdCounter;
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.closed = new AtomicBoolean(false);
        this.sstableSet = sstableSet;
        this.manifest = manifest;
    }

    public static LSMTree open(Path dataDirectory, LSMConfig config) {
        try {
            Files.createDirectories(dataDirectory);

            Manifest manifest = ManifestFile.read(dataDirectory);
            List<SSTableReader> sstables;

            if (manifest.sstables().isEmpty()) {
                sstables = loadSSTables(dataDirectory);

                if (!sstables.isEmpty()) {
                    manifest = buildManifestFromSSTables(sstables);
                    ManifestFile.write(dataDirectory, manifest);
                }
            } else {
                sstables = loadSSTablesFromManifest(dataDirectory, manifest);
            }

            Memtable memtable = new SkipListMemtable(config.memtableConfig());

            LSMTree tree = new LSMTree(
                dataDirectory,
                config,
                memtable,
                SSTableSet.of(sstables),
                manifest,
                new AtomicLong(manifest.nextSSTableId())
            );

            LeveledCompactionStrategy strategy = new LeveledCompactionStrategy(
                LeveledCompactionConfig.create()
            );
            tree.compactor = tree.new Compactor(strategy);

            return tree;
        } catch (IOException e) {
            throw new LSMException.RecoveryException("Failed to open store", e);
        }
    }

    public void put(ByteArray key, ByteArray value) {
        Entry entry = new Entry.Data(key, value);
        putEntry(entry);
    }

    public void delete(ByteArray key) {
        Entry entry = new Entry.Tombstone(key);
        putEntry(entry);
    }

    private void putEntry(Entry entry) {
        Memtable memtable = activeMemtable.get();
        memtable.put(entry);

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

            Memtable newMemtable = new SkipListMemtable(config.memtableConfig());
            long stamp = memtableLock.writeLock();
            try {
                immutableMemtables.addLast(current);
                activeMemtable.set(newMemtable);
            } finally {
                memtableLock.unlockWrite(stamp);
            }

            flushExecutor.submit(this::flushPendingMemtables);
        } finally {
            rotationLock.unlock();
        }
    }

    public Optional<ByteArray> get(ByteArray key) {
        Optional<Entry> result = activeMemtable.get().get(key);
        if (result.isPresent()) {
            return toValue(result.get());
        }

        long stamp = memtableLock.tryOptimisticRead();
        List<Memtable> immutablesCopy = List.copyOf(immutableMemtables);
        if (!memtableLock.validate(stamp)) {
            stamp = memtableLock.readLock();
            try {
                immutablesCopy = List.copyOf(immutableMemtables);
            } finally {
                memtableLock.unlockRead(stamp);
            }
        }

        for (Memtable immutable : immutablesCopy) {
            result = immutable.get(key);
            if (result.isPresent()) {
                return toValue(result.get());
            }
        }

        SSTableSet acquired = acquireSSTableSet();
        try {
            for (SSTableReader sstable : acquired.readers()) {
                result = sstable.get(key);
                if (result.isPresent()) {
                    return toValue(result.get());
                }
            }
            return Optional.empty();
        } finally {
            acquired.release();
        }
    }

    private Optional<ByteArray> toValue(Entry entry) {
        return switch (entry) {
            case Entry.Tombstone _ -> Optional.empty();
            case Entry.Data data -> Optional.of(data.value());
        };
    }

    public CloseableIterator<KeyValue> scan(ByteArray startKey, ByteArray endKey) {
        List<CloseableIterator<Entry>> iterators = new ArrayList<>();

        iterators.add(activeMemtable.get().scan(startKey, endKey));

        long stamp = memtableLock.tryOptimisticRead();
        List<Memtable> immutablesCopy = List.copyOf(immutableMemtables);
        if (!memtableLock.validate(stamp)) {
            stamp = memtableLock.readLock();
            try {
                immutablesCopy = List.copyOf(immutableMemtables);
            } finally {
                memtableLock.unlockRead(stamp);
            }
        }

        for (Memtable immutable : immutablesCopy) {
            iterators.add(immutable.scan(startKey, endKey));
        }

        SSTableSet acquired = acquireSSTableSet();
        for (SSTableReader sstable : acquired.readers()) {
            iterators.add(sstable.scan(startKey, endKey));
        }

        MergingIterator merged = new MergingIterator(iterators);
        return new KeyValueIterator(merged, acquired);
    }

    private SSTableSet acquireSSTableSet() {
        long backoffNanos = INITIAL_BACKOFF_NANOS;

        for (int attempt = 0; attempt < MAX_SSTABLE_SET_ACQUIRE_ATTEMPTS; attempt++) {
            SSTableSet current = sstableSet;
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
            "Failed to acquire SSTableSet after " + MAX_SSTABLE_SET_ACQUIRE_ATTEMPTS + " attempts"
        );
    }

    public byte[] snapshot() {
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

                for (SSTableMetadata meta : manifest.sstables()) {
                    ByteBuffer metaBuffer = ByteBuffer.allocate(meta.serializedSize());
                    meta.writeTo(metaBuffer);
                    output.write(metaBuffer.array());
                }
            } finally {
                manifestLock.unlock();
            }

            return output.toByteArray();
        } catch (IOException e) {
            throw new LSMException.SnapshotException("Failed to create snapshot", e);
        }
    }

    public void restore(byte[] data) {
        rotationLock.lock();
        try {
            awaitPendingFlushes();

            ByteBuffer buffer = ByteBuffer.wrap(data);

            long nextSSTableId = buffer.getLong();
            int sstableCount = buffer.getInt();

            List<SSTableMetadata> metadataList = new ArrayList<>(sstableCount);
            for (int i = 0; i < sstableCount; i++) {
                metadataList.add(SSTableMetadata.readFrom(buffer));
            }

            Manifest newManifest = new Manifest(nextSSTableId, metadataList);
            List<SSTableReader> newReaders = loadSSTablesFromManifest(dataDirectory, newManifest);

            manifestLock.lock();
            try {
                manifest = newManifest;
                ManifestFile.write(dataDirectory, manifest);
                sstableIdCounter.set(nextSSTableId);

                SSTableSet oldSet = sstableSet;
                List<SSTableReader> orphanedReaders = new ArrayList<>(oldSet.readers());
                sstableSet = SSTableSet.of(newReaders);
                oldSet.retire(orphanedReaders);
            } finally {
                manifestLock.unlock();
            }

            Memtable newMemtable = new SkipListMemtable(config.memtableConfig());
            long stamp = memtableLock.writeLock();
            try {
                activeMemtable.set(newMemtable);
                immutableMemtables.clear();
            } finally {
                memtableLock.unlockWrite(stamp);
            }
        } catch (Exception e) {
            throw new LSMException.SnapshotException("Failed to restore from snapshot", e);
        } finally {
            rotationLock.unlock();
        }
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

            Memtable newMemtable = new SkipListMemtable(config.memtableConfig());
            long stamp = memtableLock.writeLock();
            try {
                immutableMemtables.addLast(current);
                activeMemtable.set(newMemtable);
            } finally {
                memtableLock.unlockWrite(stamp);
            }

            flushExecutor.submit(this::flushPendingMemtables);
        } finally {
            rotationLock.unlock();
        }

        awaitPendingFlushes();
    }

    private void awaitPendingFlushes() {
        CompletableFuture<Void> sentinel = new CompletableFuture<>();
        flushExecutor.submit(() -> sentinel.complete(null));
        try {
            sentinel.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LSMException.FlushException("Interrupted waiting for flush", e);
        } catch (ExecutionException e) {
            throw new LSMException.FlushException("Flush failed", e.getCause());
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        flush();

        flushExecutor.shutdown();
        try {
            if (!flushExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                flushExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            flushExecutor.shutdownNow();
        }

        if (compactor != null) {
            compactor.close();
        }

        SSTableSet finalSet = sstableSet;
        List<SSTableReader> allReaders = new ArrayList<>(finalSet.readers());
        finalSet.retire(allReaders);

        awaitDrain(finalSet, SHUTDOWN_DRAIN_TIMEOUT);
    }

    private void awaitDrain(SSTableSet set, Duration timeout) {
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

    private void flushPendingMemtables() {
        while (true) {
            Memtable toFlush;
            long stamp = memtableLock.readLock();
            try {
                toFlush = immutableMemtables.peekFirst();
                if (toFlush == null) {
                    return;
                }
            } finally {
                memtableLock.unlockRead(stamp);
            }

            try {
                flushMemtableToSSTable(toFlush);
            } finally {
                stamp = memtableLock.writeLock();
                try {
                    immutableMemtables.pollFirst();
                } finally {
                    memtableLock.unlockWrite(stamp);
                }
                flushPermits.release();
            }
        }
    }

    private void flushMemtableToSSTable(Memtable memtable) {
        try {
            long sstableId = sstableIdCounter.incrementAndGet();
            Path sstablePath = dataDirectory.resolve(String.format("%06d.sst", sstableId));

            try (SSTableWriter writer = SSTableWriter.create(sstablePath, config.sstableConfig())) {
                Iterator<Entry> it = memtable.scan(null, null);
                while (it.hasNext()) {
                    writer.append(it.next());
                }
            }

            SSTableReader reader = SSTableReader.open(sstablePath);
            SSTableMetadata metadata = buildMetadata(reader, sstableId, 0);

            manifestLock.lock();
            try {
                List<SSTableMetadata> updatedSSTables = new ArrayList<>(manifest.sstables());
                updatedSSTables.addFirst(metadata);
                manifest = new Manifest(sstableIdCounter.get(), updatedSSTables);
                ManifestFile.write(dataDirectory, manifest);

                List<SSTableReader> newReaders = new ArrayList<>();
                newReaders.add(reader);
                newReaders.addAll(sstableSet.readers());

                SSTableSet oldSet = sstableSet;
                sstableSet = SSTableSet.of(newReaders);
                oldSet.retire(List.of());
            } finally {
                manifestLock.unlock();
            }

            compactor.maybeSchedule();
        } catch (Exception e) {
            throw new LSMException.FlushException("Failed to flush memtable to SSTable", e);
        }
    }

    private static List<SSTableReader> loadSSTables(Path dataDirectory) throws IOException {
        List<SSTableReader> sstables = new ArrayList<>();

        if (!Files.exists(dataDirectory)) {
            return sstables;
        }

        try (Stream<Path> paths = Files.list(dataDirectory)) {
            List<Path> sstablePaths = paths
                .filter(path -> SSTABLE_PATTERN.matcher(path.getFileName().toString()).matches())
                .sorted(Comparator.comparingLong(LSMTree::parseSSTableId).reversed())
                .toList();

            for (Path path : sstablePaths) {
                sstables.add(SSTableReader.open(path));
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

    private static Manifest buildManifestFromSSTables(List<SSTableReader> sstables) throws IOException {
        List<SSTableMetadata> metadataList = new ArrayList<>();
        long maxId = 0;

        for (SSTableReader reader : sstables) {
            long id = parseSSTableId(reader.path());
            maxId = Math.max(maxId, id);

            SSTableMetadata metadata = buildMetadata(reader, id, 0);
            metadataList.add(metadata);
        }

        return new Manifest(maxId, metadataList);
    }

    private static List<SSTableReader> loadSSTablesFromManifest(Path dataDirectory, Manifest manifest) {
        List<SSTableMetadata> sorted = manifest.sstables().stream()
            .sorted(Comparator.comparingLong(SSTableMetadata::id).reversed())
            .toList();

        List<SSTableReader> readers = new ArrayList<>();
        for (SSTableMetadata metadata : sorted) {
            Path path = dataDirectory.resolve(String.format("%06d.sst", metadata.id()));
            readers.add(SSTableReader.open(path));
        }

        return readers;
    }

    private static SSTableMetadata buildMetadata(SSTableReader reader, long id, int level) throws IOException {
        ByteArray smallestKey = reader.index().entries().getFirst().firstKey();
        ByteArray largestKey = reader.largestKey();
        long fileSize = Files.size(reader.path());
        long entryCount = reader.entryCount();

        return new SSTableMetadata(id, level, smallestKey, largestKey, fileSize, entryCount);
    }

    public Manifest manifest() {
        manifestLock.lock();
        try {
            return manifest;
        } finally {
            manifestLock.unlock();
        }
    }

    private void commitCompaction(List<SSTableMetadata> oldMeta, List<SSTableMetadata> newMeta) {
        manifestLock.lock();
        try {
            List<SSTableMetadata> updated = new ArrayList<>(manifest.sstables());
            updated.removeAll(oldMeta);
            updated.addAll(newMeta);
            manifest = new Manifest(manifest.nextSSTableId(), updated);
            ManifestFile.write(dataDirectory, manifest);

            Set<Long> oldIds = oldMeta.stream()
                .map(SSTableMetadata::id)
                .collect(Collectors.toSet());

            List<SSTableReader> orphanedReaders = new ArrayList<>();
            List<SSTableReader> retainedReaders = new ArrayList<>();

            for (SSTableReader reader : sstableSet.readers()) {
                if (oldIds.contains(parseSSTableId(reader.path()))) {
                    orphanedReaders.add(reader);
                } else {
                    retainedReaders.add(reader);
                }
            }

            for (SSTableMetadata meta : newMeta) {
                Path path = dataDirectory.resolve(String.format("%06d.sst", meta.id()));
                retainedReaders.add(SSTableReader.open(path));
            }

            SSTableSet oldSet = sstableSet;
            sstableSet = SSTableSet.of(retainedReaders);
            oldSet.retire(orphanedReaders);
        } finally {
            manifestLock.unlock();
        }
    }

    private long allocateSSTableId() {
        manifestLock.lock();
        try {
            long nextId = sstableIdCounter.incrementAndGet();
            manifest = new Manifest(nextId, manifest.sstables());
            return nextId;
        } finally {
            manifestLock.unlock();
        }
    }

    private Path resolvePath(long id) {
        return dataDirectory.resolve(String.format("%06d.sst", id));
    }

    private class Compactor {

        private static final long TARGET_SSTABLE_SIZE = 2 * 1024 * 1024;

        private final CompactionStrategy strategy;
        private final ExecutorService executor;
        private final AtomicBoolean compacting;
        private volatile boolean closed;

        Compactor(CompactionStrategy strategy) {
            this.strategy = strategy;
            this.executor = Executors.newVirtualThreadPerTaskExecutor();
            this.compacting = new AtomicBoolean(false);
            this.closed = false;
        }

        void maybeSchedule() {
            if (closed) {
                return;
            }

            if (!compacting.compareAndSet(false, true)) {
                return;
            }

            executor.submit(() -> {
                try {
                    run();
                } finally {
                    compacting.set(false);
                }
            });
        }

        private void run() {
            Manifest current = manifest();
            Optional<CompactionTask> task = strategy.selectCompaction(current);

            if (task.isEmpty()) {
                return;
            }

            try {
                execute(task.get());
            } catch (Exception e) {
                logger.error("Compaction failed", e);
            }
        }

        private void execute(CompactionTask task) throws IOException {
            List<SSTableReader> readers = openSSTables(task.inputs());

            try {
                List<CloseableIterator<Entry>> iterators = readers.stream()
                    .map(r -> r.scan(null, null))
                    .toList();

                MergingIterator mergeIterator = new MergingIterator(iterators);

                List<SSTableMetadata> newSSTables = writeMergedSSTables(
                    mergeIterator,
                    task.targetLevel(),
                    task.isBottomLevel()
                );

                commitCompaction(task.inputs(), newSSTables);

                deleteOldSSTables(task.inputs());

                maybeSchedule();

            } catch (Exception e) {
                readers.forEach(SSTableReader::close);
                throw e;
            }
        }

        private List<SSTableReader> openSSTables(List<SSTableMetadata> metadata) {
            List<SSTableReader> readers = new ArrayList<>();
            for (SSTableMetadata meta : metadata) {
                Path path = resolvePath(meta.id());
                readers.add(SSTableReader.open(path));
            }
            return readers;
        }

        private List<SSTableMetadata> writeMergedSSTables(
            Iterator<Entry> entries,
            int targetLevel,
            boolean dropTombstones
        ) throws IOException {
            List<SSTableMetadata> result = new ArrayList<>();

            while (entries.hasNext()) {
                long sstableId = allocateSSTableId();
                Path path = resolvePath(sstableId);

                try (SSTableWriter writer = SSTableWriter.create(path, config.sstableConfig())) {
                    long bytesWritten = 0;

                    while (entries.hasNext() && bytesWritten < TARGET_SSTABLE_SIZE) {
                        Entry entry = entries.next();

                        switch (entry) {
                            case Entry.Tombstone _ when dropTombstones -> {}
                            default -> {
                                writer.append(entry);
                                bytesWritten += estimateSize(entry);
                            }
                        }
                    }
                }

                SSTableReader reader = SSTableReader.open(path);
                SSTableMetadata metadata = buildCompactionMetadata(reader, sstableId, targetLevel);
                result.add(metadata);
                reader.close();
            }

            return result;
        }

        private SSTableMetadata buildCompactionMetadata(SSTableReader reader, long id, int level) throws IOException {
            ByteArray smallestKey = reader.index().entries().getFirst().firstKey();
            ByteArray largestKey = reader.largestKey();
            long fileSize = Files.size(reader.path());
            long entryCount = reader.entryCount();

            return new SSTableMetadata(id, level, smallestKey, largestKey, fileSize, entryCount);
        }

        private void deleteOldSSTables(List<SSTableMetadata> metadata) {
            for (SSTableMetadata meta : metadata) {
                Path path = resolvePath(meta.id());
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    logger.warn("Failed to delete old SSTable: {}", path);
                }
            }
        }

        private long estimateSize(Entry entry) {
            return switch (entry) {
                case Entry.Data data -> 1 + 4 + entry.key().size() + 4 + data.value().size();
                case Entry.Tombstone _ -> 1 + 4 + entry.key().size() + 4;
            };
        }

        void close() {
            closed = true;
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private static final class KeyValueIterator implements CloseableIterator<KeyValue> {

        private final MergingIterator delegate;
        private final SSTableSet sstableSet;
        private KeyValue next;

        KeyValueIterator(MergingIterator delegate, SSTableSet sstableSet) {
            this.delegate = delegate;
            this.sstableSet = sstableSet;
            advance();
        }

        private void advance() {
            while (delegate.hasNext()) {
                switch (delegate.next()) {
                    case Entry.Data data -> {
                        next = new KeyValue(data.key(), data.value());
                        return;
                    }
                    case Entry.Tombstone _ -> {}
                }
            }
            next = null;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public KeyValue next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            KeyValue result = next;
            advance();
            return result;
        }

        @Override
        public void close() {
            delegate.close();
            sstableSet.release();
        }
    }
}
