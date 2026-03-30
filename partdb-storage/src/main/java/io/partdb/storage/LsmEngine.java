package io.partdb.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
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

final class LsmEngine implements AutoCloseable {

    private static final int MAX_IMMUTABLE_MEMTABLES = 4;

    private final LsmConfig config;
    private final AtomicReference<Memtable> activeMemtable;
    private final ReentrantLock immutableMemtablesLock;
    private volatile List<Memtable> immutableMemtables;
    private final ReentrantLock rotationLock;
    private final Semaphore flushPermits;
    private final ExecutorService flushExecutor;
    private final AtomicBoolean closed;
    private final SSTableStore sstableStore;

    private LsmEngine(LsmConfig config, SSTableStore sstableStore) {
        this.config = config;
        this.sstableStore = sstableStore;
        this.activeMemtable = new AtomicReference<>(new Memtable());
        this.immutableMemtablesLock = new ReentrantLock();
        this.immutableMemtables = List.of();
        this.rotationLock = new ReentrantLock();
        this.flushPermits = new Semaphore(MAX_IMMUTABLE_MEMTABLES);
        this.flushExecutor = Executors.newSingleThreadExecutor(Thread.ofVirtual().factory());
        this.closed = new AtomicBoolean(false);
    }

    static LsmEngine open(Path dataDirectory, LsmConfig config) {
        try {
            Files.createDirectories(dataDirectory);
            SSTableStore sstableStore = SSTableStore.open(dataDirectory, config);
            return new LsmEngine(config, sstableStore);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open store", e);
        }
    }

    void put(Slice key, Slice value, long revision) {
        Mutation mutation = new Mutation.Put(key, value, revision);
        applyMutation(mutation);
    }

    void delete(Slice key, long revision) {
        Mutation mutation = new Mutation.Tombstone(key, revision);
        applyMutation(mutation);
    }

    Optional<EngineEntry> get(Slice key) {
        Optional<Mutation> result = lookupMutation(key, activeMemtable.get(), immutableMemtables);
        if (result.isPresent()) {
            return resolveEntry(result.get());
        }

        try (SSTableView readers = sstableStore.acquire()) {
            return readers.get(key).flatMap(this::resolveEntry);
        }
    }

    EngineEntryCursor scan(Slice startKey, Slice endKey) {
        SSTableView readers = sstableStore.acquire();
        try {
            List<Iterator<Mutation>> iterators = new ArrayList<>();

            iterators.add(activeMemtable.get().scan(startKey, endKey));
            addImmutableMemtableIterators(iterators, startKey, endKey, immutableMemtables);

            for (SSTable sstable : readers.scanTables(startKey, endKey)) {
                SSTable.Scan scan = sstable.scan();
                if (startKey != null) {
                    scan = scan.from(startKey);
                }
                if (endKey != null) {
                    scan = scan.until(endKey);
                }
                iterators.add(scan.iterator());
            }

            return new ScanCursor(readers, new MergingIterator(iterators));
        } catch (RuntimeException e) {
            readers.close();
            throw e;
        }
    }

    byte[] checkpoint() {
        flush();
        return sstableStore.checkpoint();
    }

    void restoreFromCheckpoint(byte[] data) {
        rotationLock.lock();
        try {
            awaitPendingFlushes();
            sstableStore.restore(data);
            resetMemtables();
        } finally {
            rotationLock.unlock();
        }
    }

    SSTableManifest manifest() {
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

            Memtable newMemtable = new Memtable();
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

        if (memtable.sizeInBytes() >= config.memtableMaxSizeBytes()) {
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
            if (current.sizeInBytes() < config.memtableMaxSizeBytes()) {
                return;
            }

            try {
                flushPermits.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageException.IO("Interrupted waiting for flush permit", e);
            }

            Memtable newMemtable = new Memtable();
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

    private Optional<EngineEntry> resolveEntry(Mutation mutation) {
        return switch (mutation) {
            case Mutation.Tombstone _ -> Optional.empty();
            case Mutation.Put p -> Optional.of(new EngineEntry(p.key(), p.value(), p.revision()));
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
        activeMemtable.set(new Memtable());
    }

    static Optional<Mutation> lookupMutation(Slice key, Memtable activeMemtable, List<Memtable> immutableMemtables) {
        Optional<Mutation> result = activeMemtable.get(key);
        if (result.isPresent()) {
            return result;
        }

        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            result = immutableMemtables.get(i).get(key);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    private static void addImmutableMemtableIterators(
        List<Iterator<Mutation>> iterators,
        Slice startKey,
        Slice endKey,
        List<Memtable> immutableMemtables
    ) {
        for (int i = immutableMemtables.size() - 1; i >= 0; i--) {
            iterators.add(immutableMemtables.get(i).scan(startKey, endKey));
        }
    }

    private static final class ScanCursor implements EngineEntryCursor {
        private final SSTableView readers;
        private final MergingIterator merged;
        private EngineEntry next;
        private boolean closed;

        private ScanCursor(SSTableView readers, MergingIterator merged) {
            this.readers = readers;
            this.merged = merged;
            this.next = advance();
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public EngineEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            EngineEntry result = next;
            next = advance();
            return result;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                readers.close();
            }
        }

        private EngineEntry advance() {
            while (merged.hasNext()) {
                if (merged.next() instanceof Mutation.Put p) {
                    return new EngineEntry(p.key(), p.value(), p.revision());
                }
            }
            return null;
        }
    }
}
