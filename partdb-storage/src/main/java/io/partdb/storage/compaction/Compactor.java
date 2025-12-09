package io.partdb.storage.compaction;

import io.partdb.common.Timestamp;
import io.partdb.storage.Entry;
import io.partdb.storage.MergingIterator;
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
        boolean gcTombstones,
        Timestamp oldestActiveSnapshot
    ) {
        List<Iterator<Entry>> iterators = sources.stream()
            .map(table -> table.scan().allVersions().iterator())
            .toList();

        Iterator<Entry> merged = new MergingIterator(iterators);
        Iterator<Entry> filtered = gcTombstones
            ? new TombstoneFilter(merged, oldestActiveSnapshot)
            : merged;

        return writeOutputs(filtered, targetLevel);
    }

    private List<SSTableInfo> writeOutputs(Iterator<Entry> entries, int targetLevel) {
        List<SSTableInfo> outputs = new ArrayList<>();

        while (entries.hasNext()) {
            long id = idAllocator.getAsLong();
            Path path = pathResolver.apply(id);

            Timestamps timestamps = writeSSTable(entries, path);
            SSTableInfo info = readInfo(id, path, targetLevel, timestamps);
            outputs.add(info);
        }

        return outputs;
    }

    private Timestamps writeSSTable(Iterator<Entry> entries, Path path) {
        Timestamp smallest = null;
        Timestamp largest = null;
        long bytesWritten = 0;

        try (SSTable.Writer writer = SSTable.Writer.create(path, sstableConfig)) {
            while (entries.hasNext() && bytesWritten < config.targetSSTableSize()) {
                Entry entry = entries.next();
                writer.add(entry);
                bytesWritten += estimateEntrySize(entry);

                if (smallest == null || entry.timestamp().compareTo(smallest) < 0) {
                    smallest = entry.timestamp();
                }
                if (largest == null || entry.timestamp().compareTo(largest) > 0) {
                    largest = entry.timestamp();
                }
            }
        }

        return new Timestamps(smallest, largest);
    }

    private SSTableInfo readInfo(long id, Path path, int level, Timestamps timestamps) {
        try (SSTable table = SSTable.open(path)) {
            return new SSTableInfo(
                id,
                level,
                table.smallestKey().toByteArray(),
                table.largestKey().toByteArray(),
                timestamps.smallest(),
                timestamps.largest(),
                Files.size(path),
                table.entryCount()
            );
        } catch (IOException e) {
            throw new CompactionException("Failed to read SSTable info: " + path, e);
        }
    }

    private static long estimateEntrySize(Entry entry) {
        int keySize = entry.key().length();
        return switch (entry) {
            case Entry.Put put -> 1 + 8 + 4 + keySize + 4 + put.value().length();
            case Entry.Tombstone _ -> 1 + 8 + 4 + keySize + 4;
        };
    }

    private record Timestamps(Timestamp smallest, Timestamp largest) {}

    private static final class TombstoneFilter implements Iterator<Entry> {

        private final Iterator<Entry> source;
        private final Timestamp oldestActiveSnapshot;
        private Entry next;

        TombstoneFilter(Iterator<Entry> source, Timestamp oldestActiveSnapshot) {
            this.source = source;
            this.oldestActiveSnapshot = oldestActiveSnapshot;
            advance();
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
            advance();
            return result;
        }

        private void advance() {
            while (source.hasNext()) {
                Entry entry = source.next();

                boolean isTombstone = entry instanceof Entry.Tombstone;
                boolean safeToGC = entry.timestamp().compareTo(oldestActiveSnapshot) < 0;

                if (isTombstone && safeToGC) {
                    continue;
                }

                next = entry;
                return;
            }
            next = null;
        }
    }
}
