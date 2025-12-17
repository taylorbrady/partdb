package io.partdb.storage;

import io.partdb.common.Entry;
import io.partdb.common.Slice;
import io.partdb.storage.manifest.Manifest;
import io.partdb.storage.memtable.Memtable;
import io.partdb.storage.memtable.SkipListMemtable;
import io.partdb.storage.sstable.ReadSet;
import io.partdb.storage.sstable.SSTable;
import io.partdb.storage.sstable.SSTableStore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class LSMTree implements AutoCloseable {

    private static final int MAX_IMMUTABLE_MEMTABLES = 4;

    private final LSMConfig config;
    private final AtomicReference<Memtable> activeMemtable;
    private final ReentrantLock immutableMemtablesLock;
    private volatile List<Memtable> immutableMemtables;
    private final ReentrantLock rotationLock;
    private final Semaphore flushPermits;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean closed;
    private final SSTableStore sstableStore;

    private LSMTree(LSMConfig config, SSTableStore sstableStore) {
        this.config = config;
        this.sstableStore = sstableStore;
        this.activeMemtable = new AtomicReference<>(new SkipListMemtable());
        this.immutableMemtablesLock = new ReentrantLock();
        this.immutableMemtables = List.of();
        this.rotationLock = new ReentrantLock();
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.closed = new AtomicBoolean(false);
    }

    public static LSMTree open(Path dataDirectory, LSMConfig config) {
        try {
            Files.createDirectories(dataDirectory);
            SSTableStore sstableStore = SSTableStore.open(dataDirectory, config.sstableConfig());
            return new LSMTree(config, sstableStore);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open store", e);
        }
    }

    public void put(Slice key, Slice value, long revision) {
        Mutation mutation = new Mutation.Put(key, value, revision);
        applyMutation(mutation);
    }

    public void delete(Slice key, long revision) {
        Mutation mutation = new Mutation.Tombstone(key, revision);
        applyMutation(mutation);
    }

    public Optional<Entry> get(Slice key) {
        Optional<Mutation> result = activeMemtable.get().get(key);
        if (result.isPresent()) {
            return resolveEntry(result.get());
        }

        for (Memtable immutable : immutableMemtables) {
            result = immutable.get(key);
            if (result.isPresent()) {
                return resolveEntry(result.get());
            }
        }

        try (ReadSet readers = sstableStore.acquire()) {
            for (SSTable sstable : readers.all()) {
                result = sstable.get(key);
                if (result.isPresent()) {
                    return resolveEntry(result.get());
                }
            }
            return Optional.empty();
        }
    }

    public Stream<Entry> scan(Slice startKey, Slice endKey) {
        List<Iterator<Mutation>> iterators = new ArrayList<>();

        iterators.add(activeMemtable.get().scan(startKey, endKey));

        for (Memtable immutable : immutableMemtables) {
            iterators.add(immutable.scan(startKey, endKey));
        }

        ReadSet readers = sstableStore.acquire();
        for (SSTable sstable : readers.all()) {
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
            .onClose(readers::close);
    }

    public byte[] checkpoint() {
        flush();
        return sstableStore.checkpoint();
    }

    public void restoreFromCheckpoint(byte[] data) {
        rotationLock.lock();
        try {
            awaitPendingFlushes();
            sstableStore.restore(data);
            resetMemtables();
        } finally {
            rotationLock.unlock();
        }
    }

    Manifest manifest() {
        return sstableStore.manifest();
    }

    void flush() {
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
                throw new StorageException.IO("Interrupted waiting for flush permit", e);
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

        sstableStore.close();
    }

    private void applyMutation(Mutation mutation) {
        Memtable memtable = activeMemtable.get();
        memtable.put(mutation);

        if (memtable.sizeInBytes() >= config.memtableConfig().maxSizeInBytes()) {
            tryRotateMemtable(memtable);
        }
    }

    private void tryRotateMemtable(Memtable fullMemtable) {
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
                throw new StorageException.IO("Interrupted waiting for flush permit", e);
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

    private Optional<Entry> resolveEntry(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Tombstone _ -> Optional.empty();
            case Mutation.Put p -> Optional.of(new Entry(p.key(), p.value(), p.revision()));
        };
    }

    private void awaitPendingFlushes() {
        CompletableFuture<Void> sentinel = new CompletableFuture<>();
        flushExecutor.submit(() -> sentinel.complete(null));
        try {
            sentinel.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new StorageException.Timeout("Flush operation timed out", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new StorageException.IO("Interrupted waiting for flush", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof StorageException se) {
                throw se;
            }
            throw new StorageException.IO("Flush failed", cause);
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
                persistMemtable(toFlush);
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

    private void persistMemtable(Memtable memtable) {
        sstableStore.flush(memtable.scan(null, null));
    }

    private void resetMemtables() {
        immutableMemtablesLock.lock();
        try {
            immutableMemtables = List.of();
        } finally {
            immutableMemtablesLock.unlock();
        }
        activeMemtable.set(new SkipListMemtable());
    }
}
