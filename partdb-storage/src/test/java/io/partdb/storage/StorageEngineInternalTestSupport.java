package io.partdb.storage;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.concurrent.atomic.AtomicLong;

abstract class StorageEngineInternalTestSupport {

    private static final Duration COMPACTION_TIMEOUT = Duration.ofSeconds(20);

    @TempDir
    Path tempDir;

    private final AtomicLong revisionCounter = new AtomicLong(0);

    protected long nextRevision() {
        return revisionCounter.incrementAndGet();
    }

    protected static Slice key(int i) {
        return Slice.copyOf(new byte[]{(byte) i});
    }

    protected static Slice key(String s) {
        return Slice.utf8(s);
    }

    protected static Slice value(int i) {
        return Slice.copyOf(new byte[]{(byte) i});
    }

    protected static Slice value(String s) {
        return Slice.utf8(s);
    }

    protected static Slice largeValue(int size) {
        return Slice.copyOf(new byte[size]);
    }

    protected static Bytes bytes(Slice slice) {
        return Bytes.copyOf(slice.toByteArray());
    }

    protected static StorageOptions smallWriteBufferOptions(long sizeBytes) {
        return StorageOptions.builder()
            .writeBufferMaxBytes(sizeBytes)
            .build();
    }

    protected static long maxBytesForLevel(StorageOptions options, int level) {
        if (level == 0) {
            return Long.MAX_VALUE;
        }
        CompactionOptions compaction = options.compactionOptions();
        return compaction.maxBytesForLevelBase() * (long) Math.pow(compaction.levelMultiplier(), level - 1);
    }

    protected static List<EntryRecord> readAll(Scan cursor) {
        try (cursor) {
            List<EntryRecord> entries = new ArrayList<>();
            for (EntryRecord entry : cursor) {
                entries.add(entry);
            }
            return entries;
        }
    }

    protected static Optional<ValueRecord> get(StorageEngine store, Slice key) {
        return store.get(bytes(key));
    }

    protected static void put(StorageEngine store, Slice key, Slice value, long revision) {
        store.apply(new Revision(revision), Mutation.put(bytes(key), bytes(value)));
    }

    protected static void delete(StorageEngine store, Slice key, long revision) {
        store.apply(new Revision(revision), Mutation.delete(bytes(key)));
    }

    protected static void drainToDurableState(StorageEngine store) {
        store.checkpoint();
    }

    protected static void awaitCompaction(StorageEngine store) {
        awaitCompaction(store, () -> true);
    }

    protected static void awaitCompaction(StorageEngine store, BooleanSupplier postCondition) {
        long deadlineNanos = System.nanoTime() + COMPACTION_TIMEOUT.toNanos();
        while (System.nanoTime() < deadlineNanos) {
            StorageStats stats = store.stats();
            if (stats.completedCompactions() > 0
                && stats.activeCompactions() == 0
                && stats.immutableMemtableCount() == 0
                && postCondition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("Interrupted waiting for compaction", e);
            }
        }
        throw new AssertionError("Timed out waiting for compaction");
    }

    protected static SSTableManifest readManifest(Path directory) {
        return new ManifestStore(directory).read();
    }
}
