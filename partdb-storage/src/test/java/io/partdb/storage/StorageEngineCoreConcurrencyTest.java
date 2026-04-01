package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineCoreConcurrencyTest extends StorageEngineCoreTestSupport {

    @Test
    void concurrentReads() throws Exception {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            tree.flush();

            int threadCount = 10;
            int readsPerThread = 100;
            AtomicBoolean failed = new AtomicBoolean(false);

            try (var executor = Executors.newFixedThreadPool(threadCount)) {
                for (int t = 0; t < threadCount; t++) {
                    executor.submit(() -> {
                        for (int i = 0; i < readsPerThread; i++) {
                            int keyIndex = i % 100;
                            Optional<StoredEntry.Value> result = tree.get(key(keyIndex));
                            if (result.isEmpty() || !result.get().value().equals(value(keyIndex))) {
                                failed.set(true);
                            }
                        }
                    });
                }

                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Concurrent read tasks did not complete in time");
            }

            assertFalse(failed.get());
        }
    }

    @Test
    void concurrentWrites() throws Exception {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            int threadCount = 10;
            int writesPerThread = 100;

            try (var executor = Executors.newFixedThreadPool(threadCount)) {
                for (int t = 0; t < threadCount; t++) {
                    int threadId = t;
                    executor.submit(() -> {
                        for (int i = 0; i < writesPerThread; i++) {
                            int keyIndex = threadId * writesPerThread + i;
                            put(tree, key(keyIndex & 0xFF), value(keyIndex), nextRevision());
                        }
                    });
                }

                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Concurrent write tasks did not complete in time");
            }

            tree.flush();

            List<StoredEntry.Value> entries = readAll(tree.scan(ScanBounds.all()));
            assertFalse(entries.isEmpty());
        }
    }

    @Test
    void concurrentReadsAndWrites() throws Exception {
        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(i), value(i), nextRevision());
            }

            int writerCount = 5;
            int readerCount = 5;
            AtomicBoolean failed = new AtomicBoolean(false);

            try (var executor = Executors.newFixedThreadPool(writerCount + readerCount)) {
                for (int t = 0; t < writerCount; t++) {
                    int threadId = t;
                    executor.submit(() -> {
                        for (int i = 0; i < 100; i++) {
                            put(tree, key((50 + threadId * 100 + i) & 0xFF), value(i), nextRevision());
                        }
                    });
                }

                for (int t = 0; t < readerCount; t++) {
                    executor.submit(() -> {
                        try {
                            for (int i = 0; i < 100; i++) {
                                tree.get(key(i % 50));
                            }
                        } catch (Exception e) {
                            failed.set(true);
                        }
                    });
                }

                executor.shutdown();
                assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                    "Concurrent read/write tasks did not complete in time");
            }

            assertFalse(failed.get());
        }
    }

    @Test
    void readsWhileFlushing() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(i), value(i), nextRevision());
            }

            int readerCount = 5;
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicBoolean failed = new AtomicBoolean(false);

            try (var executor = Executors.newFixedThreadPool(readerCount + 1)) {
                for (int t = 0; t < readerCount; t++) {
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < 200; i++) {
                                tree.get(key(i % 50));
                            }
                        } catch (Exception e) {
                            failed.set(true);
                        }
                    });
                }

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int round = 0; round < 5; round++) {
                            for (int i = 0; i < 20; i++) {
                                put(tree, key((100 + round * 20 + i) & 0xFF), largeValue(100), nextRevision());
                            }
                            tree.flush();
                        }
                    } catch (Exception e) {
                        failed.set(true);
                    }
                });

                startLatch.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS),
                    "Read/flush tasks did not complete in time");
            }

            assertFalse(failed.get());
        }
    }

    @Test
    void readsWhileCompacting() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 50; i++) {
                    put(tree, key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                }
                tree.flush();
            }

            int readerCount = 5;
            AtomicBoolean failed = new AtomicBoolean(false);

            try (var executor = Executors.newFixedThreadPool(readerCount)) {
                for (int t = 0; t < readerCount; t++) {
                    executor.submit(() -> {
                        try {
                            for (int round = 0; round < 50; round++) {
                                for (int i = 0; i < 50; i++) {
                                    Optional<StoredEntry.Value> result = tree.get(key(String.format("key-%03d", i)));
                                    if (result.isEmpty()) {
                                        failed.set(true);
                                    }
                                }
                                Thread.sleep(10);
                            }
                        } catch (Exception e) {
                            failed.set(true);
                        }
                    });
                }

                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS),
                    "Read tasks during compaction did not complete in time");
            }

            assertFalse(failed.get());
        }
    }

    @Test
    void scanWhileFlushing() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngineCore tree = StorageEngineCore.open(tempDir, config)) {
            for (int i = 0; i < 50; i++) {
                put(tree, key(i), value(i), nextRevision());
            }

            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicBoolean failed = new AtomicBoolean(false);

            try (var executor = Executors.newFixedThreadPool(2)) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int round = 0; round < 10; round++) {
                            try (StoredValueCursor cursor = tree.scan(ScanBounds.all())) {
                                while (cursor.hasNext()) {
                                    cursor.next();
                                }
                            }
                        }
                    } catch (Exception e) {
                        failed.set(true);
                    }
                });

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int round = 0; round < 5; round++) {
                            for (int i = 0; i < 20; i++) {
                                put(tree, key((100 + round * 20 + i) & 0xFF), largeValue(100), nextRevision());
                            }
                            tree.flush();
                        }
                    } catch (Exception e) {
                        failed.set(true);
                    }
                });

                startLatch.countDown();
                executor.shutdown();
                assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS),
                    "Scan/flush tasks did not complete in time");
            }

            assertFalse(failed.get());
        }
    }
}
