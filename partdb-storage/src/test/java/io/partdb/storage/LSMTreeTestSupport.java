package io.partdb.storage;

import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

abstract class LSMTreeTestSupport {

    @TempDir
    Path tempDir;

    private final AtomicLong revisionCounter = new AtomicLong(0);

    protected long nextRevision() {
        return revisionCounter.incrementAndGet();
    }

    protected static Slice key(int i) {
        return Slice.of(new byte[]{(byte) i});
    }

    protected static Slice key(String s) {
        return Slice.of(s.getBytes(StandardCharsets.UTF_8));
    }

    protected static Slice value(int i) {
        return Slice.of(new byte[]{(byte) i});
    }

    protected static Slice value(String s) {
        return Slice.of(s.getBytes(StandardCharsets.UTF_8));
    }

    protected static Slice largeValue(int size) {
        return Slice.of(new byte[size]);
    }

    protected static LSMConfig smallMemtableConfig(int sizeBytes) {
        return LSMConfig.defaults().withMemtableMaxSizeBytes(sizeBytes);
    }

    protected static List<StorageEntry> readAll(StorageEntryCursor cursor) {
        try (cursor) {
            List<StorageEntry> entries = new ArrayList<>();
            while (cursor.hasNext()) {
                entries.add(cursor.next());
            }
            return entries;
        }
    }
}
