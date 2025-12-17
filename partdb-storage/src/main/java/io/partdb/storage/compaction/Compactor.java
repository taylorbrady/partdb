package io.partdb.storage.compaction;

import io.partdb.storage.MergingIterator;
import io.partdb.storage.Mutation;
import io.partdb.storage.sstable.SSTable;
import io.partdb.storage.sstable.SSTableConfig;
import io.partdb.storage.sstable.SSTableDescriptor;
import io.partdb.storage.sstable.SSTableStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

public final class Compactor {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);

    private final SSTableStore sstableStore;
    private final SSTableConfig config;

    public Compactor(SSTableStore sstableStore, SSTableConfig config) {
        this.sstableStore = Objects.requireNonNull(sstableStore, "sstableStore");
        this.config = Objects.requireNonNull(config, "config");
    }

    public CompactionResult compact(CompactionTask task) {
        List<SSTable> inputs = null;
        List<SSTableDescriptor> completedOutputs = new ArrayList<>();

        try {
            inputs = openSSTables(task.inputs());

            List<SSTableDescriptor> outputs = merge(inputs, task.targetLevel(), task.gcTombstones(), completedOutputs);

            return new CompactionResult.Success(task, outputs);
        } catch (Exception e) {
            logger.error("Compaction failed for task targeting level {}", task.targetLevel(), e);
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
        while (entries.hasNext()) {
            try (SSTable.Builder builder = sstableStore.createBuilder(targetLevel)) {
                while (entries.hasNext() && builder.uncompressedBytes() < config.targetUncompressedSize()) {
                    builder.add(entries.next());
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
                logger.warn("Failed to clean up compaction output: {}", desc.id(), e);
            }
        }
    }

    private void closeAll(List<SSTable> tables) {
        for (SSTable table : tables) {
            try {
                table.close();
            } catch (Exception e) {
                logger.warn("Failed to close SSTable", e);
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
