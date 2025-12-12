package io.partdb.storage.compaction;

import io.partdb.storage.MergingIterator;
import io.partdb.storage.Mutation;
import io.partdb.storage.manifest.SSTableInfo;
import io.partdb.storage.sstable.SSTable;
import io.partdb.storage.sstable.SSTableConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;

public final class Compactor {

    private final LongSupplier idAllocator;
    private final LongFunction<Path> pathResolver;
    private final SSTableConfig sstableConfig;
    private final CompactionConfig config;

    public Compactor(
        LongSupplier idAllocator,
        LongFunction<Path> pathResolver,
        SSTableConfig sstableConfig,
        CompactionConfig config
    ) {
        this.idAllocator = Objects.requireNonNull(idAllocator, "idAllocator");
        this.pathResolver = Objects.requireNonNull(pathResolver, "pathResolver");
        this.sstableConfig = Objects.requireNonNull(sstableConfig, "sstableConfig");
        this.config = Objects.requireNonNull(config, "config");
    }

    public List<SSTableInfo> execute(
        List<SSTable> sources,
        int targetLevel,
        boolean gcTombstones
    ) {
        List<Iterator<Mutation>> iterators = sources.stream()
            .map(table -> table.scan().iterator())
            .toList();

        Iterator<Mutation> merged = new MergingIterator(iterators);
        Iterator<Mutation> filtered = gcTombstones
            ? new TombstoneFilter(merged)
            : merged;

        return writeOutputs(filtered, targetLevel);
    }

    private List<SSTableInfo> writeOutputs(Iterator<Mutation> entries, int targetLevel) {
        List<SSTableInfo> outputs = new ArrayList<>();

        while (entries.hasNext()) {
            long id = idAllocator.getAsLong();
            Path path = pathResolver.apply(id);

            Revisions revisions = writeSSTable(entries, path);
            SSTableInfo info = readInfo(id, path, targetLevel, revisions);
            outputs.add(info);
        }

        return outputs;
    }

    private Revisions writeSSTable(Iterator<Mutation> entries, Path path) {
        long smallest = Long.MAX_VALUE;
        long largest = Long.MIN_VALUE;
        long bytesWritten = 0;
        boolean hasEntry = false;

        try (SSTable.Writer writer = SSTable.Writer.create(path, sstableConfig)) {
            while (entries.hasNext() && bytesWritten < config.targetSSTableSize()) {
                Mutation mutation = entries.next();
                writer.add(mutation);
                bytesWritten += estimateEntrySize(mutation);

                if (mutation.revision() < smallest) {
                    smallest = mutation.revision();
                }
                if (mutation.revision() > largest) {
                    largest = mutation.revision();
                }
                hasEntry = true;
            }
        }

        return hasEntry ? new Revisions(smallest, largest) : new Revisions(0, 0);
    }

    private SSTableInfo readInfo(long id, Path path, int level, Revisions revisions) {
        try (SSTable table = SSTable.open(path)) {
            return new SSTableInfo(
                id,
                level,
                table.smallestKey().toByteArray(),
                table.largestKey().toByteArray(),
                revisions.smallest(),
                revisions.largest(),
                Files.size(path),
                table.entryCount()
            );
        } catch (IOException e) {
            throw new CompactionException("Failed to read SSTable info: " + path, e);
        }
    }

    private static long estimateEntrySize(Mutation mutation) {
        int keySize = mutation.key().length();
        return switch (mutation) {
            case Mutation.Put put -> 1 + 8 + 4 + keySize + 4 + put.value().length();
            case Mutation.Tombstone _ -> 1 + 8 + 4 + keySize + 4;
        };
    }

    private record Revisions(long smallest, long largest) {}

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
