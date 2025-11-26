package io.partdb.storage.compaction;

import io.partdb.common.ByteArray;
import io.partdb.storage.MergingIterator;
import io.partdb.storage.Store;
import io.partdb.storage.StoreEntry;
import io.partdb.storage.sstable.SSTableReader;
import io.partdb.storage.sstable.SSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CompactionExecutor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionExecutor.class);
    private static final long TARGET_SSTABLE_SIZE = 2 * 1024 * 1024;

    private final Store engine;
    private final CompactionStrategy strategy;
    private final ExecutorService executor;
    private final AtomicBoolean compacting;
    private volatile boolean closed;

    public CompactionExecutor(Store engine, CompactionStrategy strategy) {
        this.engine = engine;
        this.strategy = strategy;
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
        this.compacting = new AtomicBoolean(false);
        this.closed = false;
    }

    public void maybeScheduleCompaction() {
        if (closed) {
            return;
        }

        if (!compacting.compareAndSet(false, true)) {
            return;
        }

        executor.submit(() -> {
            try {
                runCompaction();
            } finally {
                compacting.set(false);
            }
        });
    }

    private void runCompaction() {
        ManifestData manifest = engine.getManifest();
        Optional<CompactionTask> task = strategy.selectCompaction(manifest);

        if (task.isEmpty()) {
            return;
        }

        try {
            executeCompaction(task.get());
        } catch (Exception e) {
            logger.error("Compaction failed", e);
        }
    }

    private void executeCompaction(CompactionTask task) throws IOException {
        List<SSTableReader> readers = openSSTables(task.inputs());

        try {
            List<Iterator<StoreEntry>> iterators = readers.stream()
                .map(r -> r.scan(null, null))
                .toList();

            MergingIterator mergeIterator = new MergingIterator(iterators);

            List<SSTableMetadata> newSSTables = writeMergedSSTables(
                mergeIterator,
                task.targetLevel()
            );

            engine.swapSSTables(task.inputs(), newSSTables);

            readers.forEach(SSTableReader::close);

            deleteOldSSTables(task.inputs());

        } catch (Exception e) {
            readers.forEach(SSTableReader::close);
            throw e;
        }
    }

    private List<SSTableReader> openSSTables(List<SSTableMetadata> metadata) {
        List<SSTableReader> readers = new ArrayList<>();
        for (SSTableMetadata meta : metadata) {
            Path path = engine.sstablePath(meta.id());
            readers.add(SSTableReader.open(path));
        }
        return readers;
    }

    private List<SSTableMetadata> writeMergedSSTables(
        Iterator<StoreEntry> entries,
        int targetLevel
    ) throws IOException {
        List<SSTableMetadata> result = new ArrayList<>();

        while (entries.hasNext()) {
            long sstableId = engine.nextSSTableId();
            Path path = engine.sstablePath(sstableId);

            try (SSTableWriter writer = SSTableWriter.create(path, engine.sstableConfig())) {
                long bytesWritten = 0;

                while (entries.hasNext() && bytesWritten < TARGET_SSTABLE_SIZE) {
                    StoreEntry entry = entries.next();

                    if (entry.tombstone() && targetLevel == 6) {
                        continue;
                    }

                    writer.append(entry);
                    bytesWritten += estimateSize(entry);
                }
            }

            SSTableReader reader = SSTableReader.open(path);
            SSTableMetadata metadata = buildMetadata(reader, sstableId, targetLevel);
            result.add(metadata);
            reader.close();
        }

        return result;
    }

    private SSTableMetadata buildMetadata(SSTableReader reader, long id, int level) throws IOException {
        ByteArray smallestKey = reader.index().entries().getFirst().firstKey();
        ByteArray largestKey = reader.largestKey();
        long fileSize = Files.size(reader.path());
        long entryCount = reader.entryCount();

        return new SSTableMetadata(id, level, smallestKey, largestKey, fileSize, entryCount);
    }

    private void deleteOldSSTables(List<SSTableMetadata> metadata) {
        for (SSTableMetadata meta : metadata) {
            Path path = engine.sstablePath(meta.id());
            try {
                Files.deleteIfExists(path);
            } catch (IOException e) {
                logger.warn("Failed to delete old SSTable: {}", path);
            }
        }
    }

    private long estimateSize(StoreEntry entry) {
        long size = 1 + 4 + entry.key().size() + 4;
        if (entry.value() != null) {
            size += entry.value().size();
        }
        return size;
    }

    @Override
    public void close() {
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
