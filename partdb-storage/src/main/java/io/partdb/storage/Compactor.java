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

    private final SSTableStore sstableStore;
    private final LSMConfig config;

    Compactor(SSTableStore sstableStore, LSMConfig config) {
        this.sstableStore = Objects.requireNonNull(sstableStore, "sstableStore");
        this.config = Objects.requireNonNull(config, "config");
    }

    CompactionResult compact(CompactionTask task) {
        long startNanos = System.nanoTime();
        List<SSTable> inputs = null;
        List<SSTableDescriptor> completedOutputs = new ArrayList<>();

        try {
            inputs = openSSTables(task.inputs());

            List<SSTableDescriptor> outputs = merge(inputs, task.targetLevel(), task.gcTombstones(), completedOutputs);

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

    private List<SSTable> openSSTables(List<SSTableDescriptor> descriptors) {
        List<SSTable> readers = new ArrayList<>();
        try {
            for (SSTableDescriptor desc : descriptors) {
                readers.add(sstableStore.openForCompaction(desc));
            }
            return readers;
        } catch (Exception e) {
            closeAll(readers);
            throw e;
        }
    }

    private List<SSTableDescriptor> merge(
        List<SSTable> sources,
        int targetLevel,
        boolean gcTombstones,
        List<SSTableDescriptor> completedOutputs
    ) {
        List<Iterator<Mutation>> iterators = sources.stream()
            .map(table -> table.scan().iterator())
            .toList();

        Iterator<Mutation> merged = new MergingIterator(iterators);
        Iterator<Mutation> filtered = gcTombstones
            ? new TombstoneFilter(merged)
            : merged;

        return writeOutputs(filtered, targetLevel, completedOutputs);
    }

    private List<SSTableDescriptor> writeOutputs(
        Iterator<Mutation> entries,
        int targetLevel,
        List<SSTableDescriptor> completedOutputs
    ) {
        Mutation pending = null;

        while (pending != null || entries.hasNext()) {
            try (SSTable.Builder builder = sstableStore.createBuilder(targetLevel)) {
                while (pending != null || entries.hasNext()) {
                    Mutation next = pending != null ? pending : entries.next();
                    pending = null;

                    if (builder.uncompressedBytes() > 0
                        && builder.uncompressedBytes() + next.sizeInBytes() > config.targetUncompressedSize()) {
                        pending = next;
                        break;
                    }

                    builder.add(next);
                }
                completedOutputs.add(builder.finish());
            }
        }

        return completedOutputs;
    }

    private void cleanupOutputs(List<SSTableDescriptor> outputs) {
        for (SSTableDescriptor desc : outputs) {
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
