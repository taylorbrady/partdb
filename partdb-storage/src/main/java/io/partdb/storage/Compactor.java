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
    private static final int GRANDPARENT_OVERLAP_FACTOR = 10;

    private record Reservation(Set<Long> ids, int sourceLevel, int targetLevel, Slice smallest, Slice largest) {
        boolean conflicts(Reservation other) {
            boolean sharedLevel = sourceLevel == other.sourceLevel || sourceLevel == other.targetLevel
                || targetLevel == other.sourceLevel || targetLevel == other.targetLevel;
            boolean overlapping = largest.compareTo(other.smallest) >= 0 && other.largest.compareTo(smallest) >= 0;
            return (sharedLevel && overlapping) || ids.stream().anyMatch(other.ids::contains);
        }
    }

    private final Store store;
    private final StoreOptions options;
    private final CompactionPlanner planner;
    private final ExecutorService executor = Executors.newThreadPerTaskExecutor(
        Thread.ofVirtual().name("partdb-compaction-", 0).factory()
    );
    private final ReentrantLock lock = new ReentrantLock();
    private final List<Reservation> reservations = new ArrayList<>();
    private final AtomicLong completed = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private volatile Duration lastDuration = Duration.ZERO;
    private volatile boolean closed;

    Compactor(Store store, StoreOptions options) {
        this.store = store;
        this.options = options;
        this.planner = new CompactionPlanner(options.compaction());
    }

    void request() {
        lock.lock();
        try {
            if (closed || reservations.size() >= options.compaction().maxConcurrent()) {
                return;
            }
            ReadState state = store.acquire();
            try {
                Version version = state.version();
                for (CompactionPlanner.Task task : planner.plan(version.metadata(), reservedIds())) {
                    if (reservations.size() >= options.compaction().maxConcurrent()) {
                        break;
                    }
                    var reservation = new Reservation(
                        new HashSet<>(task.inputs().stream().map(TableMetadata::id).toList()),
                        task.sourceLevel(),
                        task.targetLevel(),
                        task.smallest(),
                        task.largest()
                    );
                    if (reservations.stream().noneMatch(reservation::conflicts)) {
                        reservations.add(reservation);
                        Version pinned = version.retain();
                        Generation generation = store.generation();
                        executor.execute(() -> run(task, reservation, pinned, generation));
                    }
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
            return reservations.size();
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

    private Set<Long> reservedIds() {
        Set<Long> ids = new HashSet<>();
        reservations.forEach(reservation -> ids.addAll(reservation.ids()));
        return ids;
    }

    private void run(CompactionPlanner.Task task, Reservation reservation, Version version, Generation generation) {
        long started = System.nanoTime();
        List<TableReader> outputs = new ArrayList<>();
        boolean succeeded = false;
        try {
            List<TableReader> inputs = task.inputs().stream().map(input -> version.table(input.id()).orElseThrow()).toList();
            merge(task, inputs, generation, outputs);
            if (store.installCompaction(inputs, outputs, task.targetLevel())) {
                completed.incrementAndGet();
            } else {
                outputs.forEach(TableReader::markObsolete);
            }
            succeeded = true;
        } catch (RuntimeException e) {
            outputs.forEach(TableReader::markObsolete);
            if (!closed) {
                failed.incrementAndGet();
                log.atError()
                    .setCause(e)
                    .addKeyValue("targetLevel", task.targetLevel())
                    .addKeyValue("inputs", task.inputs().size())
                    .log("Compaction failed");
            }
        } finally {
            outputs.forEach(TableReader::release);
            version.release();
            lastDuration = Duration.ofNanos(System.nanoTime() - started);
            lock.lock();
            try {
                reservations.remove(reservation);
            } finally {
                lock.unlock();
            }
        }
        if (succeeded) {
            request();
        }
    }

    private void merge(CompactionPlanner.Task task, List<TableReader> inputs, Generation generation, List<TableReader> outputs) {
        List<Iterator<InternalEntry>> sources = inputs.stream().map(input -> input.scan(Bounds.ALL)).toList();
        Iterator<InternalEntry> entries = new LatestIterator(new MergingIterator(sources), Long.MAX_VALUE, task.dropTombstones());
        long target = options.compaction().targetTableBytes();
        TableWriter writer = null;
        Slice firstKey = null;
        try {
            while (entries.hasNext()) {
                if (closed) {
                    throw new StoreException.Closed();
                }
                InternalEntry entry = entries.next();
                if (writer != null && (writer.rawBytes() + entry.encodedSize() > target
                    || grandparentOverlap(task, firstKey, entry.userKey()) > target * GRANDPARENT_OVERLAP_FACTOR)) {
                    writer.finish();
                    outputs.add(open(writer, generation, task.targetLevel()));
                    writer = null;
                }
                if (writer == null) {
                    long id = generation.allocateTableId();
                    writer = TableWriter.create(generation.tablePath(id), id, task.targetLevel(), options.table());
                    firstKey = entry.userKey();
                }
                writer.add(entry);
            }
            if (writer != null) {
                writer.finish();
                outputs.add(open(writer, generation, task.targetLevel()));
                writer = null;
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
        if (!outputs.isEmpty()) {
            Disk.syncDirectory(generation.directory());
        }
    }

    private TableReader open(TableWriter writer, Generation generation, int level) {
        return TableReader.open(generation.tablePath(writer.id()), writer.id(), level, store.cache());
    }

    private static long grandparentOverlap(CompactionPlanner.Task task, Slice start, Slice end) {
        long bytes = 0;
        for (TableMetadata grandparent : task.grandparents()) {
            if (grandparent.overlaps(start, end)) {
                bytes += grandparent.sizeBytes();
            }
        }
        return bytes;
    }
}
