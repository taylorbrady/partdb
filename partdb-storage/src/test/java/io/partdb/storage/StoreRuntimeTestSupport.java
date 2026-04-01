package io.partdb.storage;

import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

abstract class StoreRuntimeTestSupport {

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

    protected static LsmConfig smallMemtableConfig(int sizeBytes) {
        return LsmConfig.defaults().withMemtableMaxSizeBytes(sizeBytes);
    }

    protected static List<StoredEntry.Value> readAll(StoredValueCursor cursor) {
        try (cursor) {
            List<StoredEntry.Value> entries = new ArrayList<>();
            while (cursor.hasNext()) {
                entries.add(cursor.next());
            }
            return entries;
        }
    }

    protected static void awaitCompaction(StoreRuntime store) {
        store.awaitCompactionIdle(COMPACTION_TIMEOUT);
    }
}
