package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsConcurrencyTest extends StorageEngineInternalTestSupport {

    @Test
    void concurrentReads() throws Exception {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            drainToDurableState(tree);

            int threadCount = 10;
            int readsPerThread = 100;

            try (var executor = Executors.newFixedThreadPool(threadCount)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threadCount; t++) {
                    futures.add(executor.submit(() -> {
                        for (int i = 0; i < readsPerThread; i++) {
                            int keyIndex = i % 100;
                            Optional<ValueRecord> result = get(tree, key(keyIndex));
                            assertTrue(result.isPresent(), "missing key " + keyIndex);
                            assertEquals(bytes(value(keyIndex)), result.get().value());
                        }
                    }));
                }

                awaitAll(executor, futures, 10);
            }
        }
    }

    @Test
    void concurrentWrites() throws Exception {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            int threadCount = 10;
            int writesPerThread = 100;

            try (var executor = Executors.newFixedThreadPool(threadCount)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < threadCount; t++) {
                    int threadId = t;
                    futures.add(executor.submit(() -> {
                        for (int i = 0; i < writesPerThread; i++) {
                            put(tree, writerKey(threadId, i), value(i), nextRevision());
                        }
                    }));
                }

                awaitAll(executor, futures, 10);
            }

            drainToDurableState(tree);

            List<EntryRecord> entries = readAll(tree.scan(KeyRange.all()));
            assertEquals(threadCount * writesPerThread, entries.size());
            for (int t = 0; t < threadCount; t++) {
                for (int i = 0; i < writesPerThread; i++) {
                    Optional<ValueRecord> result = get(tree, writerKey(t, i));
                    assertTrue(result.isPresent(), "missing key for writer " + t + " index " + i);
                    assertEquals(bytes(value(i)), result.get().value());
                }
            }
        }
    }

    @Test
    void concurrentReadsAndWrites() throws Exception {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(i), value(i), nextRevision());
            }

            int writerCount = 5;
            int readerCount = 5;

            try (var executor = Executors.newFixedThreadPool(writerCount + readerCount)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < writerCount; t++) {
                    int threadId = t;
                    futures.add(executor.submit(() -> {
                        for (int i = 0; i < 100; i++) {
                            put(tree, writerKey(threadId, i), value(i), nextRevision());
                        }
                    }));
                }

                for (int t = 0; t < readerCount; t++) {
                    futures.add(executor.submit(() -> {
                        for (int i = 0; i < 100; i++) {
                            Optional<ValueRecord> result = get(tree, key(i % 50));
                            assertTrue(result.isPresent(), "missing key " + (i % 50));
                        }
                    }));
                }

                awaitAll(executor, futures, 10);
            }
        }
    }

    @Test
    void readsWhileFlushing() throws Exception {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(i), value(i), nextRevision());
            }

            int readerCount = 5;
            CountDownLatch startLatch = new CountDownLatch(1);

            try (var executor = Executors.newFixedThreadPool(readerCount + 1)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < readerCount; t++) {
                    futures.add(executor.submit(() -> {
                        startLatch.await();
                        for (int i = 0; i < 200; i++) {
                            Optional<ValueRecord> result = get(tree, key(i % 50));
                            assertTrue(result.isPresent(), "missing key " + (i % 50));
                        }
                        return null;
                    }));
                }

                futures.add(executor.submit(() -> {
                    startLatch.await();
                    for (int round = 0; round < 5; round++) {
                        for (int i = 0; i < 20; i++) {
                            put(tree, key(100 + round * 20 + i), largeValue(100), nextRevision());
                        }
                        drainToDurableState(tree);
                    }
                    return null;
                }));

                startLatch.countDown();
                awaitAll(executor, futures, 30);
            }
        }
    }

    @Test
    void readsWhileCompacting() throws Exception {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 50; i++) {
                    put(tree, key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                }
                drainToDurableState(tree);
            }

            int readerCount = 5;

            try (var executor = Executors.newFixedThreadPool(readerCount)) {
                List<Future<?>> futures = new ArrayList<>();
                for (int t = 0; t < readerCount; t++) {
                    futures.add(executor.submit(() -> {
                        for (int round = 0; round < 50; round++) {
                            for (int i = 0; i < 50; i++) {
                                Optional<ValueRecord> result = get(tree, key(String.format("key-%03d", i)));
                                assertTrue(result.isPresent(), "missing key " + i);
                            }
                            Thread.sleep(10);
                        }
                        return null;
                    }));
                }

                awaitAll(executor, futures, 30);
            }
        }
    }

    @Test
    void scanWhileFlushing() throws Exception {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(i), value(i), nextRevision());
            }

            CountDownLatch startLatch = new CountDownLatch(1);

            try (var executor = Executors.newFixedThreadPool(2)) {
                List<Future<?>> futures = new ArrayList<>();
                futures.add(executor.submit(() -> {
                    startLatch.await();
                    for (int round = 0; round < 10; round++) {
                        try (Scan cursor = tree.scan(KeyRange.all())) {
                            for (EntryRecord ignored : cursor) {
                                // drain
                            }
                        }
                    }
                    return null;
                }));

                futures.add(executor.submit(() -> {
                    startLatch.await();
                    for (int round = 0; round < 5; round++) {
                        for (int i = 0; i < 20; i++) {
                            put(tree, key(100 + round * 20 + i), largeValue(100), nextRevision());
                        }
                        drainToDurableState(tree);
                    }
                    return null;
                }));

                startLatch.countDown();
                awaitAll(executor, futures, 30);
            }
        }
    }

    private static Slice writerKey(int writerId, int index) {
        return key(String.format("writer-%d-%03d", writerId, index));
    }

    private static void awaitAll(ExecutorService executor, List<Future<?>> futures, long timeoutSeconds)
        throws Exception {
        executor.shutdown();
        assertTrue(executor.awaitTermination(timeoutSeconds, TimeUnit.SECONDS),
            "Concurrent tasks did not complete in time");
        for (Future<?> future : futures) {
            future.get();
        }
    }
}
