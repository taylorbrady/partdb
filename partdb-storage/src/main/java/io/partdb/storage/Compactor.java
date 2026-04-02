package io.partdb.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

final class Compactor {

    private static final Logger log = LoggerFactory.getLogger(Compactor.class);
    private static final int GRANDPARENT_OVERLAP_MULTIPLIER = 10;

    private final SstableStore sstableStore;
    private final VersionSet versionSet;
    private final LsmConfig config;

    Compactor(SstableStore sstableStore, VersionSet versionSet, LsmConfig config) {
        this.sstableStore = Objects.requireNonNull(sstableStore, "sstableStore");
        this.versionSet = Objects.requireNonNull(versionSet, "versionSet");
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
                readers.add(sstableStore.openCompactionReader(table));
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
        List<Iterator<InternalEntry>> iterators = sources.stream()
            .map(table -> table.scan(ScanBounds.all()))
            .toList();

        Iterator<InternalEntry> merged = new CompactionIterator(
            new InternalEntryMergingIterator(iterators),
            versionSet.oldestSnapshotRevision(),
            gcTombstones
        );
        return writeOutputs(merged, targetLevel, grandparents, completedOutputs);
    }

    private List<SSTableMetadata> writeOutputs(
        Iterator<InternalEntry> entries,
        int targetLevel,
        List<SSTableMetadata> grandparents,
        List<SSTableMetadata> completedOutputs
    ) {
        InternalEntry pending = null;

        while (pending != null || entries.hasNext()) {
            try (SSTableWriter writer = sstableStore.createWriter(versionSet.allocateSstableId(), targetLevel)) {
                Slice firstUserKey = null;
                Slice lastUserKey = null;
                while (pending != null || entries.hasNext()) {
                    InternalEntry next = pending != null ? pending : entries.next();
                    pending = null;

                    if (shouldFinishOutput(writer, firstUserKey, lastUserKey, next, grandparents)) {
                        pending = next;
                        break;
                    }

                    writer.add(next);
                    if (firstUserKey == null) {
                        firstUserKey = next.userKey();
                    }
                    lastUserKey = next.userKey();
                }
                completedOutputs.add(writer.finish());
            }
        }

        return completedOutputs;
    }

    private boolean shouldFinishOutput(
        SSTableWriter writer,
        Slice firstUserKey,
        Slice lastUserKey,
        InternalEntry next,
        List<SSTableMetadata> grandparents
    ) {
        if (writer.uncompressedBytes() == 0 || firstUserKey == null) {
            return false;
        }

        if (lastUserKey != null && lastUserKey.equals(next.userKey())) {
            return false;
        }

        if (writer.uncompressedBytes() + next.encodedSizeBytes() > config.targetUncompressedSize()) {
            return true;
        }

        return grandparentOverlapBytes(firstUserKey, next.userKey(), grandparents) > maxGrandparentOverlapBytes();
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
        sstableStore.deleteTables(outputs);
    }

    private void closeAll(List<SSTableReader> tables) {
        sstableStore.closeReaders(tables);
    }
}
