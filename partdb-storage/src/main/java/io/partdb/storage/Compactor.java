package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

final class Compactor {

    private static final Logger log = LoggerFactory.getLogger(Compactor.class);
    private static final int GRANDPARENT_OVERLAP_MULTIPLIER = 10;

    private final SSTableStore sstableStore;
    private final LSMConfig config;

    Compactor(SSTableStore sstableStore, LSMConfig config) {
        this.sstableStore = Objects.requireNonNull(sstableStore, "sstableStore");
        this.config = Objects.requireNonNull(config, "config");
    }

    CompactionResult compact(CompactionTask task) {
        long startNanos = System.nanoTime();
        List<SSTable> inputs = null;
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

    private List<SSTable> openSSTables(List<SSTableMetadata> metadata) {
        List<SSTable> readers = new ArrayList<>();
        try {
            for (SSTableMetadata table : metadata) {
                readers.add(sstableStore.openForCompaction(table));
            }
            return readers;
        } catch (Exception e) {
            closeAll(readers);
            throw e;
        }
    }

    private List<SSTableMetadata> merge(
        List<SSTable> sources,
        int targetLevel,
        boolean gcTombstones,
        List<SSTableMetadata> grandparents,
        List<SSTableMetadata> completedOutputs
    ) {
        List<Iterator<Mutation>> iterators = sources.stream()
            .map(table -> table.scan().iterator())
            .toList();

        Iterator<Mutation> merged = new MergingIterator(iterators);
        Iterator<Mutation> filtered = gcTombstones
            ? new TombstoneFilter(merged)
            : merged;

        return writeOutputs(filtered, targetLevel, grandparents, completedOutputs);
    }

    private List<SSTableMetadata> writeOutputs(
        Iterator<Mutation> entries,
        int targetLevel,
        List<SSTableMetadata> grandparents,
        List<SSTableMetadata> completedOutputs
    ) {
        Mutation pending = null;

        while (pending != null || entries.hasNext()) {
            try (SSTable.Builder builder = sstableStore.createBuilder(targetLevel)) {
                Slice firstKey = null;
                while (pending != null || entries.hasNext()) {
                    Mutation next = pending != null ? pending : entries.next();
                    pending = null;

                    if (shouldFinishOutput(builder, firstKey, next, grandparents)) {
                        pending = next;
                        break;
                    }

                    builder.add(next);
                    if (firstKey == null) {
                        firstKey = next.key();
                    }
                }
                completedOutputs.add(builder.finish());
            }
        }

        return completedOutputs;
    }

    private boolean shouldFinishOutput(
        SSTable.Builder builder,
        Slice firstKey,
        Mutation next,
        List<SSTableMetadata> grandparents
    ) {
        if (builder.uncompressedBytes() == 0 || firstKey == null) {
            return false;
        }

        if (builder.uncompressedBytes() + next.sizeInBytes() > config.targetUncompressedSize()) {
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
                sstableStore.delete(desc.id());
            } catch (IOException e) {
                log.atWarn()
                    .addKeyValue("sstableId", desc.id())
                    .setCause(e)
                    .log("Failed to clean up compaction output");
            }
        }
    }

    private void closeAll(List<SSTable> tables) {
        for (SSTable table : tables) {
            try {
                table.close();
            } catch (Exception e) {
                log.atWarn()
                    .setCause(e)
                    .log("Failed to close SSTable");
            }
        }
    }

    private static final class TombstoneFilter implements Iterator<Mutation> {

        private final Iterator<Mutation> source;
        private Mutation next;

        TombstoneFilter(Iterator<Mutation> source) {
            this.source = source;
            advance();
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Mutation next() {
            if (next == null) {
                throw new NoSuchElementException();
            }
            Mutation result = next;
            advance();
            return result;
        }

        private void advance() {
            while (source.hasNext()) {
                Mutation mutation = source.next();
                if (!(mutation instanceof Mutation.Tombstone)) {
                    next = mutation;
                    return;
                }
            }
            next = null;
        }
    }
}
