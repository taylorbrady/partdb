package io.partdb.storage;

import io.partdb.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

public final class Store implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Store.class);

    private final Path root;
    private final StoreOptions options;
    private final BlockCache cache;
    private final AtomicReference<ReadState> current;
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ReentrantLock editLock = new ReentrantLock();
    private final Flusher flusher;
    private final Compactor compactor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong checkpoints = new AtomicLong();
    private final AtomicLong restores = new AtomicLong();
    private volatile Duration lastCheckpoint = Duration.ZERO;
    private volatile Duration lastRestore = Duration.ZERO;
    private volatile Generation generation;
    private volatile long durableIndex;

    private Store(Path root, StoreOptions options, BlockCache cache, Generation generation, long durableIndex, Version version) {
        this.root = root;
        this.options = options;
        this.cache = cache;
        this.generation = generation;
        this.durableIndex = durableIndex;
        this.current = new AtomicReference<>(new ReadState(durableIndex, new Memtable(durableIndex), List.of(), version));
        this.flusher = new Flusher(this::flush);
        this.compactor = new Compactor(this, options);
    }

    public static Store open(Path directory, StoreOptions options) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(options, "options must not be null");
        Generation.Recovered recovered = Generation.recover(directory);
        BlockCache cache = new BlockCache(options.blockCacheMaxBytes());
        List<TableReader> tables = new ArrayList<>();
        Version version;
        try {
            for (Manifest.Placement placement : recovered.manifest().tables()) {
                Path path = recovered.generation().tablePath(placement.id());
                tables.add(TableReader.open(path, placement.id(), placement.level(), cache));
            }
            version = Version.of(tables, options.compaction().maxLevels());
        } finally {
            tables.forEach(TableReader::release);
        }
        Store store = new Store(directory, options, cache, recovered.generation(), recovered.manifest().durableIndex(), version);
        store.compactor.request();
        return store;
    }

    public long appliedIndex() {
        return current.get().index();
    }

    public long durableIndex() {
        return durableIndex;
    }

    public void apply(long index, List<Mutation> mutations) {
        Objects.requireNonNull(mutations, "mutations must not be null");
        ensureOpen();
        flusher.throwIfFailed();
        ReadState state = current.get();
        if (index <= state.index()) {
            throw new IllegalArgumentException(
                "index %d must be greater than applied index %d".formatted(index, state.index())
            );
        }
        requireUniqueKeys(mutations);
        Memtable active = state.active();
        for (Mutation mutation : mutations) {
            active.put(InternalEntry.of(mutation, index));
        }
        active.advanceTo(index);
        publish(previous -> previous.advancedTo(index));
        if (active.sizeBytes() >= options.memtableMaxBytes()) {
            rotate();
        }
    }

    public Optional<KeyValue> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        ReadState state = pin();
        try {
            return state.find(Slice.of(key)).flatMap(InternalEntry::visible);
        } finally {
            state.release();
        }
    }

    public Snapshot snapshot() {
        return new StoreSnapshot(pin());
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
            for (TableReader table : state.version().tables()) {
                parts.add(new Checkpoint.Part(table.metadata().level(), table.contents()));
            }
            Checkpoint.write(target, state.index(), parts);
        } catch (IOException e) {
            throw new StoreException.IO("Failed to write checkpoint", e);
        } finally {
            unflushed.ifPresent(TableReader::release);
            state.release();
        }
        checkpoints.incrementAndGet();
        lastCheckpoint = Duration.ofNanos(System.nanoTime() - started);
        return state.index();
    }

    public long restore(InputStream checkpoint) {
        Objects.requireNonNull(checkpoint, "checkpoint must not be null");
        ensureOpen();
        long started = System.nanoTime();
        flusher.awaitIdle();
        long index;
        editLock.lock();
        try {
            Generation staged = Generation.stage(root);
            Checkpoint.Restored restored;
            try {
                restored = Checkpoint.read(checkpoint, staged, cache, options.compaction().maxLevels());
            } catch (RuntimeException e) {
                discard(staged, e);
                throw e;
            }
            try {
                staged.activate();
            } catch (RuntimeException e) {
                restored.version().release();
                throw e;
            }

            Generation previous = generation;
            ReadState replaced;
            stateLock.lock();
            try {
                replaced = current.get();
                current.set(new ReadState(restored.index(), new Memtable(restored.index()), List.of(), restored.version()));
                generation = staged;
                durableIndex = restored.index();
            } finally {
                stateLock.unlock();
            }
            replaced.version().tables().forEach(TableReader::markObsolete);
            replaced.release();
            try {
                previous.delete();
            } catch (StoreException e) {
                log.atWarn().setCause(e).log("Failed to delete replaced storage generation");
            }
            index = restored.index();
        } finally {
            editLock.unlock();
        }
        restores.incrementAndGet();
        lastRestore = Duration.ofNanos(System.nanoTime() - started);
        compactor.request();
        return index;
    }

    public StoreStats stats() {
        ReadState state = pin();
        try {
            return new StoreStats(
                state.active().sizeBytes(),
                state.frozen().size(),
                state.version().tableCount(),
                state.version().sizeBytes(),
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
            if (!state.active().isEmpty() || state.index() > durableIndex) {
                rotate();
            }
            flusher.awaitIdle();
        } finally {
            compactor.close();
            flusher.close();
            current.get().release();
        }
    }

    ReadState pin() {
        ensureOpen();
        return acquire();
    }

    ReadState acquire() {
        while (true) {
            ReadState state = current.get();
            if (state.tryRetain()) {
                return state;
            }
        }
    }

    Generation generation() {
        return generation;
    }

    BlockCache cache() {
        return cache;
    }

    boolean installCompaction(List<TableReader> inputs, List<TableReader> outputs, int targetLevel) {
        editLock.lock();
        try {
            Version base = current.get().version();
            if (!base.containsAll(inputs)) {
                return false;
            }
            Version next = base.withCompaction(inputs, outputs, targetLevel);
            commit(generation, durableIndex, next);
            inputs.forEach(TableReader::markObsolete);
            publish(state -> state.withVersion(next));
            return true;
        } finally {
            editLock.unlock();
        }
    }

    private void flush(Memtable memtable) {
        Generation target = generation;
        Optional<TableReader> table = Optional.empty();
        try {
            if (!memtable.isEmpty()) {
                table = Optional.of(writeLevelZero(target, memtable));
            }
            editLock.lock();
            try {
                Version base = current.get().version();
                Version next = table.isPresent() ? base.withFlushed(table.get()) : base.retain();
                commit(target, memtable.appliedIndex(), next);
                durableIndex = memtable.appliedIndex();
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

    private TableReader writeLevelZero(Generation target, Memtable memtable) {
        long id = target.allocateTableId();
        Path path = target.tablePath(id);
        try (TableWriter writer = TableWriter.create(path, id, 0, options.table())) {
            Iterator<InternalEntry> entries = new LatestIterator(memtable.scan(Bounds.ALL), Long.MAX_VALUE, false);
            while (entries.hasNext()) {
                writer.add(entries.next());
            }
            writer.finish();
        }
        Disk.syncDirectory(target.directory());
        return TableReader.open(path, id, 0, cache);
    }

    private Optional<TableReader> unflushed(ReadState state) {
        List<Iterator<InternalEntry>> sources = new ArrayList<>();
        sources.add(state.active().scan(Bounds.ALL));
        state.frozen().forEach(memtable -> sources.add(memtable.scan(Bounds.ALL)));
        Iterator<InternalEntry> entries = new LatestIterator(new MergingIterator(sources), state.index(), false);
        if (!entries.hasNext()) {
            return Optional.empty();
        }
        Path path = root.resolve("checkpoint-" + UUID.randomUUID() + Disk.TEMPORARY_SUFFIX);
        try (TableWriter writer = TableWriter.create(path, 1, 0, options.table())) {
            while (entries.hasNext()) {
                writer.add(entries.next());
            }
            writer.finish();
        }
        TableReader table = TableReader.open(path, 1, 0, cache);
        table.markObsolete();
        return Optional.of(table);
    }

    private void commit(Generation target, long durable, Version next) {
        try {
            target.commit(Manifest.of(durable, target.nextTableId(), next));
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

    private static void discard(Generation staged, RuntimeException failure) {
        try {
            staged.delete();
        } catch (StoreException e) {
            failure.addSuppressed(e);
        }
    }
}
