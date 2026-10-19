package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

final class Compactor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(Compactor.class);

    private final Store store;
    private final StoreOptions options;
    private final CompactionPlanner planner;
    private final ExecutorService executor;
    private final ReentrantLock lock = new ReentrantLock();
    private final Set<Long> busy = new HashSet<>();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private int running;
    private volatile Duration lastDuration = Duration.ZERO;
    private volatile boolean closed;

    Compactor(Store store, StoreOptions options) {
        this.store = store;
        this.options = options;
        this.planner = new CompactionPlanner(options.levelZeroCompactionTrigger(), options.levelBaseBytes(), options.targetTableBytes());
        this.executor = Executors.newFixedThreadPool(
            options.compactionConcurrency(),
            Thread.ofPlatform().name("partdb-compaction-", 0).daemon(true).factory()
        );
    }

    void request() {
        lock.lock();
        try {
            if (closed || running >= options.compactionConcurrency()) {
                return;
            }
            ReadState state = store.acquire();
            try {
                Version version = state.version();
                for (CompactionPlanner.Task task : planner.plan(version.metadata(), Set.copyOf(busy))) {
                    if (running >= options.compactionConcurrency()) {
                        break;
                    }
                    task.inputs().forEach(input -> busy.add(input.id()));
                    running++;
                    Version pinned = version.retain();
                    executor.execute(() -> run(task, pinned));
                }
            } finally {
                state.release();
            }
        } finally {
            lock.unlock();
        }
    }

    int active() {
        lock.lock();
        try {
            return running;
        } finally {
            lock.unlock();
        }
    }

    long completed() {
        return completed.get();
    }

    long failed() {
        return failed.get();
    }

    Duration lastDuration() {
        return lastDuration;
    }

    @Override
    public void close() {
        lock.lock();
        try {
            closed = true;
        } finally {
            lock.unlock();
        }
        executor.close();
    }

    private void run(CompactionPlanner.Task task, Version version) {
        long started = System.nanoTime();
        boolean installed = false;
        try {
            List<TableReader> inputs = task.inputs().stream().map(input -> version.table(input.id()).orElseThrow()).toList();
            installed = switch (task) {
                case CompactionPlanner.Move move -> store.installCompaction(inputs, inputs, move.targetLevel());
                case CompactionPlanner.Merge merge -> merge(merge, inputs);
            };
            if (installed) {
                completed.incrementAndGet();
            }
        } catch (RuntimeException e) {
            if (!closed) {
                failed.incrementAndGet();
                log.atError()
                    .setCause(e)
                    .addKeyValue("sourceLevel", task.sourceLevel())
                    .addKeyValue("targetLevel", task.targetLevel())
                    .addKeyValue("inputs", task.inputs().size())
                    .log("Compaction failed");
            }
        } finally {
            version.release();
            lastDuration = Duration.ofNanos(System.nanoTime() - started);
            lock.lock();
            try {
                task.inputs().forEach(input -> busy.remove(input.id()));
                running--;
            } finally {
                lock.unlock();
            }
        }
        if (installed) {
            request();
        }
    }

    private boolean merge(CompactionPlanner.Merge merge, List<TableReader> inputs) {
        List<TableReader> outputs = new ArrayList<>();
        try {
            write(merge, inputs, outputs);
            boolean installed = store.installCompaction(inputs, outputs, merge.targetLevel());
            if (!installed) {
                outputs.forEach(TableReader::markObsolete);
            }
            return installed;
        } catch (RuntimeException e) {
            outputs.forEach(TableReader::markObsolete);
            throw e;
        } finally {
            outputs.forEach(TableReader::release);
        }
    }

    private void write(CompactionPlanner.Merge merge, List<TableReader> inputs, List<TableReader> outputs) {
        List<Iterator<InternalEntry>> sources = inputs.stream().map(input -> input.scan(Bounds.ALL)).toList();
        Iterator<InternalEntry> entries = new LatestIterator(new MergingIterator(sources), Long.MAX_VALUE, merge.dropTombstones());
        var grandparents = new Grandparents(merge.grandparents(), options.targetTableBytes() * CompactionPlanner.GRANDPARENT_OVERLAP_FACTOR);
        StoreDirectory directory = store.directory();
        TableWriter writer = null;
        try {
            while (entries.hasNext()) {
                if (closed) {
                    throw new StoreException.Closed();
                }
                InternalEntry entry = entries.next();
                boolean split = grandparents.splitBefore(entry.userKey());
                if (writer != null && (split || writer.rawBytes() >= options.targetTableBytes())) {
                    outputs.add(finish(writer, directory));
                    writer = null;
                }
                if (writer == null) {
                    long id = directory.allocateTableId();
                    writer = TableWriter.create(directory.tablePath(id), id, options);
                }
                writer.add(entry);
            }
            if (writer != null) {
                outputs.add(finish(writer, directory));
                writer = null;
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        if (!outputs.isEmpty()) {
            directory.sync();
        }
    }

    private TableReader finish(TableWriter writer, StoreDirectory directory) {
        writer.finish();
        return TableReader.open(directory.tablePath(writer.id()), writer.id(), store.cache());
    }

    private static final class Grandparents {
        private final List<TableMetadata> tables;
        private final long limit;
        private int position;
        private long overlap;
        private boolean started;

        private Grandparents(List<TableMetadata> tables, long limit) {
            this.tables = tables;
            this.limit = limit;
        }

        boolean splitBefore(Slice key) {
            while (position < tables.size() && key.compareTo(tables.get(position).largest()) > 0) {
                if (started) {
                    overlap += tables.get(position).sizeBytes();
                }
                position++;
            }
            started = true;
            if (overlap > limit) {
                overlap = 0;
                return true;
            }
            return false;
        }
    }
}
