package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

public final class Store implements AutoCloseable {

    private static final long STALL_RECHECK_MILLIS = 100;

    private final StoreDirectory directory;
    private final StoreOptions options;
    private final BlockCache cache;
    private final AtomicReference<ReadState> current;
    private final ReentrantLock stateLock = new ReentrantLock();
    private final Condition versionChanged = stateLock.newCondition();
    private final ReentrantLock editLock = new ReentrantLock();
    private final Flusher flusher;
    private final Compactor compactor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong stalledWrites = new AtomicLong();
    private final AtomicLong checkpoints = new AtomicLong();
    private final AtomicLong restores = new AtomicLong();
    private volatile Duration lastCheckpoint = Duration.ZERO;
    private volatile Duration lastRestore = Duration.ZERO;
    private volatile long durableRevision;

    private Store(StoreDirectory directory, StoreOptions options, BlockCache cache, long durableRevision, Version version) {
        this.directory = directory;
        this.options = options;
        this.cache = cache;
        this.durableRevision = durableRevision;
        this.current = new AtomicReference<>(new ReadState(durableRevision, new Memtable(durableRevision), List.of(), version));
        this.flusher = new Flusher(this::flush);
        this.compactor = new Compactor(this, options);
    }

    public static Store open(Path directory, StoreOptions options) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(options, "options must not be null");
        StoreDirectory.Opened opened = StoreDirectory.open(directory);
        BlockCache cache = new BlockCache(options.blockCacheBytes());
        Manifest manifest = opened.manifest();
        List<List<TableReader>> levels = Version.emptyLevels();
        List<TableReader> tables = new ArrayList<>();
        Version version;
        try {
            for (Manifest.Placement placement : manifest.tables()) {
                TableReader table = TableReader.open(opened.directory().tablePath(placement.id()), placement.id(), cache);
                tables.add(table);
                levels.get(placement.level()).add(table);
            }
            version = Version.of(levels);
        } finally {
            tables.forEach(TableReader::release);
        }
        Store store = new Store(opened.directory(), options, cache, manifest.durableRevision(), version);
        store.compactor.request();
        return store;
    }

    public long appliedRevision() {
        return current.get().revision();
    }

    public long durableRevision() {
        return durableRevision;
    }

    public void apply(long revision, List<Mutation> mutations) {
        Objects.requireNonNull(mutations, "mutations must not be null");
        ensureOpen();
        flusher.throwIfFailed();
        long applied = current.get().revision();
        if (revision <= applied) {
            throw new IllegalArgumentException("revision %d must be greater than applied revision %d".formatted(revision, applied));
        }
        requireUniqueKeys(mutations);
        awaitLevelZeroCapacity();
        Memtable active = current.get().active();
        for (Mutation mutation : mutations) {
            active.put(InternalEntry.of(mutation, revision));
        }
        active.advanceTo(revision);
        publish(previous -> previous.advancedTo(revision));
        if (active.sizeBytes() >= options.memtableBytes()) {
            rotate();
        }
    }

    public Snapshot snapshot() {
        return new Snapshot(pin());
    }

    public long checkpoint(OutputStream target) {
        Objects.requireNonNull(target, "target must not be null");
        long started = System.nanoTime();
        ReadState state = pin();
        Optional<TableReader> unflushed = Optional.empty();
        try {
            unflushed = unflushed(state);
            List<Checkpoint.Part> parts = new ArrayList<>();
            unflushed.ifPresent(table -> parts.add(new Checkpoint.Part(0, table.contents())));
            Version version = state.version();
            for (int level = 0; level < Version.LEVELS; level++) {
                for (TableReader table : version.level(level)) {
                    parts.add(new Checkpoint.Part(level, table.contents()));
                }
            }
            Checkpoint.write(target, state.revision(), parts);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to write checkpoint", e);
        } finally {
            unflushed.ifPresent(TableReader::release);
            state.release();
        }
        checkpoints.incrementAndGet();
        lastCheckpoint = Duration.ofNanos(System.nanoTime() - started);
        return state.revision();
    }

    public long restore(InputStream checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        ensureOpen();
        long started = System.nanoTime();
        flusher.awaitIdle();
        long revision;
        editLock.lock();
        try {
            Checkpoint.Restored restored = Checkpoint.read(checkpoint, directory, cache);
            revision = restored.revision();
            try {
                directory.commit(revision, restored.version());
            } catch (RuntimeException e) {
                restored.version().tables().forEach(TableReader::markObsolete);
                restored.version().release();
                throw e;
            }
            ReadState replaced;
            stateLock.lock();
            try {
                replaced = current.get();
                current.set(new ReadState(revision, new Memtable(revision), List.of(), restored.version()));
                durableRevision = revision;
                versionChanged.signalAll();
            } finally {
                stateLock.unlock();
            }
            replaced.version().tables().forEach(TableReader::markObsolete);
            replaced.release();
        } finally {
            editLock.unlock();
        }
        restores.incrementAndGet();
        lastRestore = Duration.ofNanos(System.nanoTime() - started);
        compactor.request();
        return revision;
    }

    public StoreStats stats() {
        ReadState state = pin();
        try {
            Version version = state.version();
            return new StoreStats(
                state.active().sizeBytes(),
                state.frozen().size(),
                version.levelZeroCount(),
                version.tableCount(),
                version.sizeBytes(),
                stalledWrites.get(),
                compactor.active(),
                compactor.completed(),
                compactor.failed(),
                compactor.lastDuration(),
                checkpoints.get(),
                restores.get(),
                lastCheckpoint,
                lastRestore
            );
        } finally {
            state.release();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            ReadState state = current.get();
            if (!state.active().isEmpty() || state.revision() > durableRevision) {
                rotate();
            }
            flusher.awaitIdle();
        } finally {
            compactor.close();
            flusher.close();
            current.get().release();
        }
    }

    ReadState acquire() {
        while (true) {
            ReadState state = current.get();
            if (state.tryRetain()) {
                return state;
            }
        }
    }

    StoreDirectory directory() {
        return directory;
    }

    BlockCache cache() {
        return cache;
    }

    boolean installCompaction(List<TableReader> removed, List<TableReader> added, int targetLevel) {
        editLock.lock();
        try {
            Version base = current.get().version();
            if (!base.containsAll(removed)) {
                return false;
            }
            Version next = base.withCompaction(removed, added, targetLevel);
            commit(durableRevision, next);
            Set<TableReader> kept = Version.identitySet(added);
            removed.stream().filter(table -> !kept.contains(table)).forEach(TableReader::markObsolete);
            publish(state -> state.withVersion(next));
            return true;
        } finally {
            editLock.unlock();
        }
    }

    private ReadState pin() {
        ensureOpen();
        return acquire();
    }

    private void awaitLevelZeroCapacity() {
        if (current.get().version().levelZeroCount() < options.levelZeroStopTrigger()) {
            return;
        }
        stalledWrites.incrementAndGet();
        while (current.get().version().levelZeroCount() >= options.levelZeroStopTrigger()) {
            ensureOpen();
            flusher.throwIfFailed();
            compactor.request();
            stateLock.lock();
            try {
                if (current.get().version().levelZeroCount() >= options.levelZeroStopTrigger()) {
                    versionChanged.await(STALL_RECHECK_MILLIS, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StoreException.IO("Interrupted waiting for level zero compaction", e);
            } finally {
                stateLock.unlock();
            }
        }
    }

    private void flush(Memtable memtable) {
        Optional<TableReader> table = Optional.empty();
        try {
            if (!memtable.isEmpty()) {
                table = Optional.of(writeLevelZero(memtable));
            }
            editLock.lock();
            try {
                Version base = current.get().version();
                Version next = table.isPresent() ? base.withFlushed(table.get()) : base.retain();
                commit(memtable.revision(), next);
                durableRevision = memtable.revision();
                publish(state -> state.flushed(memtable, next));
            } finally {
                editLock.unlock();
            }
        } catch (RuntimeException e) {
            table.ifPresent(TableReader::markObsolete);
            throw e;
        } finally {
            table.ifPresent(TableReader::release);
        }
        compactor.request();
    }

    private TableReader writeLevelZero(Memtable memtable) {
        long id = directory.allocateTableId();
        Path path = directory.tablePath(id);
        write(path, id, new LatestIterator(memtable.scan(Bounds.ALL), Long.MAX_VALUE, false));
        directory.sync();
        return TableReader.open(path, id, cache);
    }

    private Optional<TableReader> unflushed(ReadState state) {
        List<Iterator<InternalEntry>> sources = new ArrayList<>();
        sources.add(state.active().scan(Bounds.ALL));
        state.frozen().forEach(memtable -> sources.add(memtable.scan(Bounds.ALL)));
        Iterator<InternalEntry> entries = new LatestIterator(new MergingIterator(sources), state.revision(), false);
        if (!entries.hasNext()) {
            return Optional.empty();
        }
        long id = directory.allocateTableId();
        Path path = directory.temporaryPath("checkpoint");
        write(path, id, entries);
        TableReader table = TableReader.open(path, id, cache);
        table.markObsolete();
        return Optional.of(table);
    }

    private void write(Path path, long id, Iterator<InternalEntry> entries) {
        try (TableWriter writer = TableWriter.create(path, id, options)) {
            while (entries.hasNext()) {
                writer.add(entries.next());
            }
            writer.finish();
        }
    }

    private void commit(long durable, Version next) {
        try {
            directory.commit(durable, next);
        } catch (RuntimeException e) {
            next.release();
            throw e;
        }
    }

    private void rotate() {
        flusher.reserve();
        Memtable full = current.get().active();
        publish(ReadState::rotated);
        flusher.submit(full);
    }

    private void publish(UnaryOperator<ReadState> transition) {
        ReadState previous;
        stateLock.lock();
        try {
            previous = current.get();
            current.set(transition.apply(previous));
            versionChanged.signalAll();
        } finally {
            stateLock.unlock();
        }
        previous.release();
    }

    private void ensureOpen() {
        if (closed.get()) {
            throw new StoreException.Closed();
        }
    }

    private static void requireUniqueKeys(List<Mutation> mutations) {
        Set<Bytes> keys = HashSet.newHashSet(mutations.size());
        for (Mutation mutation : mutations) {
            Objects.requireNonNull(mutation, "mutation must not be null");
            if (!keys.add(mutation.key())) {
                throw new IllegalArgumentException("Duplicate key in one apply: " + mutation.key());
            }
        }
    }
}
