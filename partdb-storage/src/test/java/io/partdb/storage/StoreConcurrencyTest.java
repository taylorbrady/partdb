package io.partdb.storage;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.partdb.storage.Fixtures.bytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreConcurrencyTest {

    private static final int KEYS = 32;
    private static final int APPLIES = 1_500;
    private static final int MAX_CHECKPOINTS = 20;
    private static final StoreOptions OPTIONS = Fixtures.SMALL.withMemtableMaxBytes(64 * 1024);

    @TempDir
    Path directory;

    @Test
    void readersAlwaysSeeWholeBatchesWhileFlushAndCompactionRun() throws Exception {
        try (Store store = Store.open(directory, OPTIONS);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            AtomicBoolean done = new AtomicBoolean();
            Future<?> writer = executor.submit(() -> {
                for (long index = 1; index <= APPLIES; index++) {
                    store.apply(index, index % 7 == 0 ? List.of() : batch(index));
                }
                done.set(true);
            });

            List<Future<Integer>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                readers.add(executor.submit(() -> {
                    int checks = 0;
                    while (!done.get() || checks == 0) {
                        try (Snapshot snapshot = store.snapshot()) {
                            assertConsistent(snapshot);
                        }
                        checks++;
                    }
                    return checks;
                }));
            }

            writer.get();
            for (Future<Integer> reader : readers) {
                assertTrue(reader.get() > 0);
            }
            assertEquals(APPLIES, store.appliedIndex());
            Fixtures.await("compaction activity", () -> store.stats().completedCompactions() > 0);
            try (Snapshot snapshot = store.snapshot()) {
                assertConsistent(snapshot);
                assertEquals(lastWrite(APPLIES, 7), Long.parseLong(Fixtures.value(snapshot, key(0))));
            }
        }
    }

    @Test
    void pinnedSnapshotIsStableAcrossFlushAndCompaction() {
        try (Store store = Store.open(directory, OPTIONS)) {
            for (long index = 1; index <= 50; index++) {
                store.apply(index, batch(index));
            }
            try (Snapshot snapshot = store.snapshot()) {
                Map<String, String> expected = contents(snapshot);
                long completed = store.stats().completedCompactions();
                for (long index = 51; index <= 600; index++) {
                    store.apply(index, batch(index));
                }
                Fixtures.await("compaction after pin", () -> store.stats().completedCompactions() > completed);

                assertEquals(50, snapshot.index());
                assertEquals(expected, contents(snapshot));
                assertConsistent(snapshot);
            }
        }
    }

    @Test
    void checkpointFromAnotherThreadCapturesExactlyItsIndex() throws Exception {
        try (Store store = Store.open(directory, OPTIONS);
             ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Future<?> writer = executor.submit(() -> {
                for (long index = 1; index <= APPLIES; index++) {
                    store.apply(index, index % 5 == 0 ? List.of() : batch(index));
                }
            });

            List<byte[]> checkpoints = new ArrayList<>();
            List<Long> indexes = new ArrayList<>();
            while (!writer.isDone() && checkpoints.size() < MAX_CHECKPOINTS) {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                indexes.add(store.checkpoint(out));
                checkpoints.add(out.toByteArray());
            }
            writer.get();

            for (int i = 0; i < checkpoints.size(); i++) {
                Path target = directory.resolve("restore-" + i);
                try (Store restored = Store.open(target, OPTIONS)) {
                    long index = restored.restore(new ByteArrayInputStream(checkpoints.get(i)));
                    assertEquals(indexes.get(i), index);
                    assertEquals(index, restored.appliedIndex());
                    try (Snapshot snapshot = restored.snapshot()) {
                        assertConsistent(snapshot);
                        if (lastWrite(index, 5) > 0) {
                            assertEquals(lastWrite(index, 5), Long.parseLong(Fixtures.value(snapshot, key(0))));
                        }
                    }
                }
            }
        }
    }

    private static List<Mutation> batch(long index) {
        List<Mutation> batch = new ArrayList<>(KEYS);
        for (int k = 0; k < KEYS; k++) {
            batch.add(Mutation.put(bytes(key(k)), bytes(Long.toString(index))));
        }
        return batch;
    }

    private static long lastWrite(long index, int emptyEvery) {
        for (long i = index; i > 0; i--) {
            if (i % emptyEvery != 0) {
                return i;
            }
        }
        return 0;
    }

    private static String key(int k) {
        return "k%03d".formatted(k);
    }

    private static void assertConsistent(Snapshot snapshot) {
        List<KeyValue> entries = new ArrayList<>();
        try (Cursor cursor = snapshot.scan(KeyRange.all())) {
            cursor.forEachRemaining(entries::add);
        }
        if (entries.isEmpty()) {
            return;
        }
        assertEquals(KEYS, entries.size());
        Bytes first = entries.getFirst().value();
        long revision = entries.getFirst().revision();
        assertTrue(revision <= snapshot.index());
        for (KeyValue entry : entries) {
            assertEquals(first, entry.value(), "batch torn at " + entry.key().utf8());
            assertEquals(revision, entry.revision());
            assertEquals(entry, snapshot.get(entry.key()).orElseThrow());
        }
    }

    private static Map<String, String> contents(Snapshot snapshot) {
        Map<String, String> contents = new HashMap<>();
        try (Cursor cursor = snapshot.scan(KeyRange.all())) {
            cursor.forEachRemaining(entry -> contents.put(entry.key().utf8(), entry.value().utf8() + "@" + entry.revision()));
        }
        return contents;
    }
}
