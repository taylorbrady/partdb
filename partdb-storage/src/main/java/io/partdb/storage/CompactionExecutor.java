package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

final class CompactionExecutor {

    private static final Logger log = LoggerFactory.getLogger(CompactionExecutor.class);
    private static final int GRANDPARENT_OVERLAP_MULTIPLIER = 10;

    private final TableCatalog tableCatalog;
    private final LsmConfig config;

    CompactionExecutor(TableCatalog tableCatalog, LsmConfig config) {
        this.tableCatalog = Objects.requireNonNull(tableCatalog, "tableCatalog");
        this.config = Objects.requireNonNull(config, "config");
    }

    CompactionResult compact(CompactionTask task) {
        long startNanos = System.nanoTime();
        List<SSTableReader> inputs = null;
        List<SSTableMetadata> completedOutputs = new ArrayList<>();

        try {
            inputs = openSSTables(task.inputs());

            List<SSTableMetadata> outputs = merge(
                inputs,
                task.targetLevel(),
                task.gcTombstones(),
                task.grandparents(),
                completedOutputs
            );

            long durationMs = (System.nanoTime() - startNanos) / 1_000_000;
            log.atInfo()
                .addKeyValue("targetLevel", task.targetLevel())
                .addKeyValue("inputFiles", task.inputs().size())
                .addKeyValue("outputFiles", outputs.size())
                .addKeyValue("durationMs", durationMs)
                .log("Compaction completed");

            return new CompactionResult.Success(task, outputs);
        } catch (Exception e) {
            log.atError()
                .addKeyValue("targetLevel", task.targetLevel())
                .addKeyValue("inputFiles", task.inputs().size())
                .setCause(e)
                .log("Compaction failed");
            cleanupOutputs(completedOutputs);
            return new CompactionResult.Failure(task, e);
        } finally {
            if (inputs != null) {
                closeAll(inputs);
            }
        }
    }

    private List<SSTableReader> openSSTables(List<SSTableMetadata> metadata) {
        List<SSTableReader> readers = new ArrayList<>();
        try {
            for (SSTableMetadata table : metadata) {
                readers.add(tableCatalog.openCompactionReader(table));
            }
            return readers;
        } catch (Exception e) {
            closeAll(readers);
            throw e;
        }
    }

    private List<SSTableMetadata> merge(
        List<SSTableReader> sources,
        int targetLevel,
        boolean gcTombstones,
        List<SSTableMetadata> grandparents,
        List<SSTableMetadata> completedOutputs
    ) {
        List<Iterator<StoredEntry>> iterators = sources.stream()
            .map(table -> table.scan(ScanBounds.all()))
            .toList();

        Iterator<StoredEntry> merged = new MergingIterator(iterators);
        Iterator<StoredEntry> filtered = gcTombstones
            ? new TombstoneFilter(merged)
            : merged;

        return writeOutputs(filtered, targetLevel, grandparents, completedOutputs);
    }

    private List<SSTableMetadata> writeOutputs(
        Iterator<StoredEntry> entries,
        int targetLevel,
        List<SSTableMetadata> grandparents,
        List<SSTableMetadata> completedOutputs
    ) {
        StoredEntry pending = null;

        while (pending != null || entries.hasNext()) {
            try (SSTableWriter writer = tableCatalog.createWriter(targetLevel)) {
                Slice firstKey = null;
                while (pending != null || entries.hasNext()) {
                    StoredEntry next = pending != null ? pending : entries.next();
                    pending = null;

                    if (shouldFinishOutput(writer, firstKey, next, grandparents)) {
                        pending = next;
                        break;
                    }

                    writer.add(next);
                    if (firstKey == null) {
                        firstKey = next.key();
                    }
                }
                completedOutputs.add(writer.finish());
            }
        }

        return completedOutputs;
    }

    private boolean shouldFinishOutput(
        SSTableWriter writer,
        Slice firstKey,
        StoredEntry next,
        List<SSTableMetadata> grandparents
    ) {
        if (writer.uncompressedBytes() == 0 || firstKey == null) {
            return false;
        }

        if (writer.uncompressedBytes() + next.encodedSizeBytes() > config.targetUncompressedSize()) {
            return true;
        }

        return grandparentOverlapBytes(firstKey, next.key(), grandparents) > maxGrandparentOverlapBytes();
    }

    private long grandparentOverlapBytes(
        Slice startKey,
        Slice endKey,
        List<SSTableMetadata> grandparents
    ) {
        long overlapBytes = 0;

        for (SSTableMetadata metadata : grandparents) {
            if (metadata.overlaps(startKey, endKey)) {
                overlapBytes += metadata.fileSizeBytes();
            }
        }

        return overlapBytes;
    }

    private long maxGrandparentOverlapBytes() {
        return config.targetUncompressedSize() * GRANDPARENT_OVERLAP_MULTIPLIER;
    }

    private void cleanupOutputs(List<SSTableMetadata> outputs) {
        for (SSTableMetadata desc : outputs) {
            try {
                tableCatalog.delete(desc.id());
            } catch (IOException e) {
                log.atWarn()
                    .addKeyValue("sstableId", desc.id())
                    .setCause(e)
                    .log("Failed to clean up compaction output");
            }
        }
    }

    private void closeAll(List<SSTableReader> tables) {
        for (SSTableReader table : tables) {
            try {
                table.close();
            } catch (Exception e) {
                log.atWarn()
                    .setCause(e)
                    .log("Failed to close SSTable");
            }
        }
    }

    private static final class TombstoneFilter implements Iterator<StoredEntry> {

        private final Iterator<StoredEntry> source;
        private StoredEntry next;

        TombstoneFilter(Iterator<StoredEntry> source) {
            this.source = source;
            advance();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public StoredEntry next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            StoredEntry result = next;
            advance();
            return result;
        }

        private void advance() {
            while (source.hasNext()) {
                StoredEntry entry = source.next();
                if (!(entry instanceof StoredEntry.Tombstone)) {
                    next = entry;
                    return;
                }
            }
            next = null;
        }
    }
}
