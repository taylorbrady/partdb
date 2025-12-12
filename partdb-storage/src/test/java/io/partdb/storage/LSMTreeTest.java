package io.partdb.storage;

import io.partdb.common.Entry;
import io.partdb.common.Slice;

import io.partdb.storage.compaction.CompactionConfig;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.manifest.Manifest;
import io.partdb.storage.manifest.SSTableInfo;
import io.partdb.storage.memtable.MemtableConfig;
import io.partdb.storage.sstable.BlockCacheConfig;
import io.partdb.storage.sstable.SSTableConfig;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class LSMTreeTest {

    @TempDir
    Path tempDir;

    private static Slice key(int i) {
        return Slice.of(new byte[]{(byte) i});
    }

    private static Slice key(String s) {
        return Slice.of(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Slice value(int i) {
        return Slice.of(new byte[]{(byte) i});
    }

    private static Slice value(String s) {
        return Slice.of(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Slice largeValue(int size) {
        return Slice.of(new byte[size]);
    }

    private static LSMConfig smallMemtableConfig(int sizeBytes) {
        return new LSMConfig(
            new MemtableConfig(sizeBytes),
            SSTableConfig.defaults(),
            CompactionConfig.defaults(),
            LeveledCompactionConfig.defaults(),
            BlockCacheConfig.defaults()
        );
    }

    private final AtomicLong revisionCounter = new AtomicLong(0);

    private long nextRevision() {
        return revisionCounter.incrementAndGet();
    }

    @Nested
    class BasicOperations {

        @Test
        void putAndGet() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());

                Optional<Entry> result = tree.get(key(1));

                assertTrue(result.isPresent());
                assertEquals(value(10), result.get().value());
            }
        }

        @Test
        void getNonExistentKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                Optional<Entry> result = tree.get(key(99));
                assertTrue(result.isEmpty());
            }
        }

        @Test
        void deleteKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.delete(key(1), nextRevision());

                Optional<Entry> result = tree.get(key(1));
                assertTrue(result.isEmpty());
            }
        }

        @Test
        void deleteNonExistentKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.delete(key(1), nextRevision());

                Optional<Entry> result = tree.get(key(1));
                assertTrue(result.isEmpty());
            }
        }

        @Test
        void putOverwritesPreviousValue() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(1), value(20), nextRevision());

                Optional<Entry> result = tree.get(key(1));

                assertTrue(result.isPresent());
                assertEquals(value(20), result.get().value());
            }
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), value(20), nextRevision());
                tree.put(key(3), value(30), nextRevision());

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    List<Entry> entries = stream.toList();

                    assertEquals(3, entries.size());
                    assertEquals(key(1), entries.get(0).key());
                    assertEquals(key(2), entries.get(1).key());
                    assertEquals(key(3), entries.get(2).key());
                }
            }
        }

        @Test
        void withBounds() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), value(20), nextRevision());
                tree.put(key(3), value(30), nextRevision());
                tree.put(key(4), value(40), nextRevision());

                try (Stream<Entry> stream = tree.scan(key(2), key(4))) {
                    List<Entry> entries = stream.toList();

                    assertEquals(2, entries.size());
                    assertEquals(key(2), entries.get(0).key());
                    assertEquals(key(3), entries.get(1).key());
                }
            }
        }

        @Test
        void excludesDeletedKeys() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), value(20), nextRevision());
                tree.delete(key(2), nextRevision());
                tree.put(key(3), value(30), nextRevision());

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    List<Entry> entries = stream.toList();

                    assertEquals(2, entries.size());
                    assertEquals(key(1), entries.get(0).key());
                    assertEquals(key(3), entries.get(1).key());
                }
            }
        }

        @Test
        void mergesFromMultipleSources() {
            LSMConfig config = smallMemtableConfig(512);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 20; i++) {
                    tree.put(key(i), largeValue(100), nextRevision());
                }

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    List<Entry> entries = stream.toList();

                    assertEquals(20, entries.size());
                    for (int i = 0; i < 20; i++) {
                        assertEquals(key(i), entries.get(i).key());
                    }
                }
            }
        }

        @Test
        void usesLatestValueForDuplicateKeys() {
            LSMConfig config = smallMemtableConfig(256);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), largeValue(100), nextRevision());
                tree.put(key(1), value(20), nextRevision());

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    List<Entry> entries = stream.toList();

                    Optional<Entry> keyEntry = entries.stream()
                        .filter(e -> e.key().equals(key(1)))
                        .findFirst();

                    assertTrue(keyEntry.isPresent());
                    assertEquals(value(20), keyEntry.get().value());
                }
            }
        }
    }

    @Nested
    class Flush {

        @Test
        void manualFlush() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), value(20), nextRevision());

                tree.flush();

                assertTrue(tree.get(key(1)).isPresent());
                assertTrue(tree.get(key(2)).isPresent());
            }
        }

        @Test
        void automaticOnMemtableSize() {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), largeValue(200), nextRevision());
                }

                for (int i = 0; i < 10; i++) {
                    assertTrue(tree.get(key(i)).isPresent());
                }
            }
        }
    }

    @Nested
    class Recovery {

        @Test
        void fromSSTables() {
            LSMConfig config = LSMConfig.defaults();

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), value(20), nextRevision());
                tree.flush();
            }

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                Optional<Entry> result1 = tree.get(key(1));
                Optional<Entry> result2 = tree.get(key(2));

                assertTrue(result1.isPresent());
                assertEquals(value(10), result1.get().value());

                assertTrue(result2.isPresent());
                assertEquals(value(20), result2.get().value());
            }
        }

        @Test
        void readPathPriority() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.flush();
                tree.put(key(1), value(20), nextRevision());

                Optional<Entry> result = tree.get(key(1));

                assertTrue(result.isPresent());
                assertEquals(value(20), result.get().value());
            }
        }

        @Test
        void multipleSSTables() {
            LSMConfig config = smallMemtableConfig(512);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(i), largeValue(100), nextRevision());
                }

                tree.flush();

                for (int i = 0; i < 30; i++) {
                    assertTrue(tree.get(key(i)).isPresent());
                }
            }
        }
    }

    @Nested
    class Compaction {

        @Test
        void l0TriggersAtThreshold() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
                }

                tree.flush();
                Thread.sleep(500);

                Manifest manifest = tree.manifest();
                List<SSTableInfo> l0Files = manifest.level(0);
                List<SSTableInfo> l1Files = manifest.level(1);

                assertTrue(l0Files.size() < 4);
                assertTrue(l1Files.size() > 0);
            }
        }

        @Test
        void mergesOverlappingKeys() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int version = 0; version < 5; version++) {
                    for (int i = 0; i < 20; i++) {
                        tree.put(key(String.format("key-%02d", i)), value("v" + version + "-" + i), nextRevision());
                    }
                    tree.flush();
                }

                Thread.sleep(1000);

                for (int i = 0; i < 20; i++) {
                    Optional<Entry> result = tree.get(key(String.format("key-%02d", i)));
                    assertTrue(result.isPresent());
                    assertTrue(new String(result.get().value().toByteArray(), StandardCharsets.UTF_8).startsWith("v4"));
                }
            }
        }

        @Test
        void tombstonesResultInEmptyGet() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
                }
                tree.flush();

                for (int i = 0; i < 50; i++) {
                    tree.delete(key(String.format("key-%03d", i)), nextRevision());
                }
                tree.flush();

                Thread.sleep(500);

                for (int i = 0; i < 50; i++) {
                    assertTrue(tree.get(key(String.format("key-%03d", i))).isEmpty());
                }
            }
        }

        @Test
        void levelSizeRespected() throws Exception {
            LSMConfig config = smallMemtableConfig(2048);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 20; batch++) {
                    for (int i = 0; i < 100; i++) {
                        tree.put(key(String.format("key-%05d", batch * 100 + i)), largeValue(100), nextRevision());
                    }
                    tree.flush();
                }

                Thread.sleep(2000);

                Manifest manifest = tree.manifest();
                LeveledCompactionConfig compactionConfig = LeveledCompactionConfig.defaults();

                for (int level = 1; level < manifest.maxLevel(); level++) {
                    long levelSize = manifest.levelSize(level);
                    long maxSize = compactionConfig.maxBytesForLevel(level);

                    assertTrue(levelSize <= maxSize * 2);
                }
            }
        }

        @Test
        void preservesNewestVersions() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                List<String> expectedValues = new ArrayList<>();

                for (int i = 0; i < 30; i++) {
                    String val = "version-" + i;
                    expectedValues.add(val);
                    tree.put(key(String.format("key-%02d", i)), value(val), nextRevision());

                    if (i % 10 == 9) {
                        tree.flush();
                    }
                }

                Thread.sleep(1000);

                for (int i = 0; i < 30; i++) {
                    Optional<Entry> result = tree.get(key(String.format("key-%02d", i)));
                    assertTrue(result.isPresent());
                    assertEquals(expectedValues.get(i), new String(result.get().value().toByteArray(), StandardCharsets.UTF_8));
                }
            }
        }

        @Test
        void manifestConsistency() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 80; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
                }
                tree.flush();

                Thread.sleep(500);

                Manifest manifest = tree.manifest();

                long totalEntries = 0;
                for (SSTableInfo meta : manifest.sstables()) {
                    totalEntries += meta.entryCount();
                    assertTrue(meta.id() > 0);
                    assertTrue(meta.level() >= 0);
                    assertTrue(meta.fileSizeBytes() > 0);
                    assertNotNull(meta.smallestKey());
                    assertNotNull(meta.largestKey());
                }

                assertTrue(totalEntries >= 80);
            }
        }

        @Test
        void reopenAfterCompaction() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            List<Slice> keys = new ArrayList<>();
            List<Slice> values = new ArrayList<>();

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 60; i++) {
                    Slice k = key(String.format("key-%03d", i));
                    Slice v = value("value-" + i);
                    keys.add(k);
                    values.add(v);
                    tree.put(k, v, nextRevision());
                }
                tree.flush();
                Thread.sleep(500);
            }

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < keys.size(); i++) {
                    Optional<Entry> result = tree.get(keys.get(i));
                    assertTrue(result.isPresent());
                    assertEquals(values.get(i), result.get().value());
                }
            }
        }

        @Test
        void scanAfterCompaction() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextRevision());
                }
                tree.flush();

                Thread.sleep(500);

                Slice startKey = key("key-020");
                Slice endKey = key("key-030");

                try (Stream<Entry> stream = tree.scan(startKey, endKey)) {
                    List<Entry> entries = stream.toList();

                    assertEquals(10, entries.size());
                    for (Entry e : entries) {
                        assertTrue(e.key().compareTo(startKey) >= 0);
                        assertTrue(e.key().compareTo(endKey) < 0);
                    }
                }
            }
        }

        @Test
        void emptyManifestLoad() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                Manifest manifest = tree.manifest();
                assertNotNull(manifest);
                assertTrue(manifest.sstables().isEmpty());
            }
        }
    }

    @Nested
    class Checkpoint {

        @Test
        void roundtrip() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.put(key(2), value(20), nextRevision());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();
                tree.restoreFromCheckpoint(checkpoint);

                assertTrue(tree.get(key(1)).isPresent());
                assertEquals(value(10), tree.get(key(1)).get().value());
                assertTrue(tree.get(key(2)).isPresent());
                assertEquals(value(20), tree.get(key(2)).get().value());
            }
        }

        @Test
        void restoresToPreviousState() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();

                tree.put(key(2), value(20), nextRevision());
                tree.put(key(3), value(30), nextRevision());
                tree.flush();

                assertTrue(tree.get(key(2)).isPresent());
                assertTrue(tree.get(key(3)).isPresent());

                tree.restoreFromCheckpoint(checkpoint);

                assertTrue(tree.get(key(1)).isPresent());
                assertEquals(value(10), tree.get(key(1)).get().value());
                assertTrue(tree.get(key(2)).isEmpty());
                assertTrue(tree.get(key(3)).isEmpty());
            }
        }

        @Test
        void capturesMultipleSSTables() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.flush();

                tree.put(key(2), value(20), nextRevision());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();
                assertEquals(2, tree.manifest().sstables().size());

                tree.put(key(3), value(30), nextRevision());
                tree.flush();

                tree.restoreFromCheckpoint(checkpoint);

                assertTrue(tree.get(key(1)).isPresent());
                assertTrue(tree.get(key(2)).isPresent());
                assertTrue(tree.get(key(3)).isEmpty());
                assertEquals(2, tree.manifest().sstables().size());
            }
        }

        @Test
        void clearsMemtableOnRestore() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();

                tree.put(key(2), value(20), nextRevision());

                assertTrue(tree.get(key(2)).isPresent());

                tree.restoreFromCheckpoint(checkpoint);

                assertTrue(tree.get(key(1)).isPresent());
                assertTrue(tree.get(key(2)).isEmpty());
            }
        }

        @Test
        void manifestStateRestored() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();
                long originalNextId = tree.manifest().nextSSTableId();
                int originalSSTableCount = tree.manifest().sstables().size();

                tree.put(key(2), value(20), nextRevision());
                tree.flush();

                assertTrue(tree.manifest().nextSSTableId() > originalNextId);

                tree.restoreFromCheckpoint(checkpoint);

                assertEquals(originalNextId, tree.manifest().nextSSTableId());
                assertEquals(originalSSTableCount, tree.manifest().sstables().size());
            }
        }

        @Test
        void multipleCheckpoints() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextRevision());
                tree.flush();
                byte[] checkpoint1 = tree.checkpoint();

                tree.put(key(2), value(20), nextRevision());
                tree.flush();
                byte[] checkpoint2 = tree.checkpoint();

                tree.put(key(3), value(30), nextRevision());
                tree.flush();

                tree.restoreFromCheckpoint(checkpoint1);
                assertTrue(tree.get(key(1)).isPresent());
                assertTrue(tree.get(key(2)).isEmpty());
                assertTrue(tree.get(key(3)).isEmpty());

                tree.restoreFromCheckpoint(checkpoint2);
                assertTrue(tree.get(key(1)).isPresent());
                assertTrue(tree.get(key(2)).isPresent());
                assertTrue(tree.get(key(3)).isEmpty());
            }
        }

        @Test
        void emptyTree() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                byte[] checkpoint = tree.checkpoint();

                tree.put(key(1), value(10), nextRevision());
                tree.flush();
                assertTrue(tree.get(key(1)).isPresent());

                tree.restoreFromCheckpoint(checkpoint);

                assertTrue(tree.get(key(1)).isEmpty());
                assertTrue(tree.manifest().sstables().isEmpty());
            }
        }
    }

    @Nested
    class Concurrency {

        @Test
        void concurrentReads() throws Exception {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(i), value(i), nextRevision());
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
                                Optional<Entry> result = tree.get(key(keyIndex));
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
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                int threadCount = 10;
                int writesPerThread = 100;

                try (var executor = Executors.newFixedThreadPool(threadCount)) {
                    for (int t = 0; t < threadCount; t++) {
                        int threadId = t;
                        executor.submit(() -> {
                            for (int i = 0; i < writesPerThread; i++) {
                                int keyIndex = threadId * writesPerThread + i;
                                tree.put(key(keyIndex & 0xFF), value(keyIndex), nextRevision());
                            }
                        });
                    }

                    executor.shutdown();
                    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                        "Concurrent write tasks did not complete in time");
                }

                tree.flush();

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    List<Entry> entries = stream.toList();
                    assertFalse(entries.isEmpty());
                }
            }
        }

        @Test
        void concurrentReadsAndWrites() throws Exception {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i), nextRevision());
                }

                int writerCount = 5;
                int readerCount = 5;
                AtomicBoolean failed = new AtomicBoolean(false);

                try (var executor = Executors.newFixedThreadPool(writerCount + readerCount)) {
                    for (int t = 0; t < writerCount; t++) {
                        int threadId = t;
                        executor.submit(() -> {
                            for (int i = 0; i < 100; i++) {
                                tree.put(key((50 + threadId * 100 + i) & 0xFF), value(i), nextRevision());
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
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i), nextRevision());
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
                                    tree.put(key((100 + round * 20 + i) & 0xFF), largeValue(100), nextRevision());
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
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 50; i++) {
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
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
                                        Optional<Entry> result = tree.get(key(String.format("key-%03d", i)));
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
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i), nextRevision());
                }

                CountDownLatch startLatch = new CountDownLatch(1);
                AtomicBoolean failed = new AtomicBoolean(false);

                try (var executor = Executors.newFixedThreadPool(2)) {
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            for (int round = 0; round < 10; round++) {
                                try (Stream<Entry> stream = tree.scan(null, null)) {
                                    stream.forEach(_ -> {});
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
                                    tree.put(key((100 + round * 20 + i) & 0xFF), largeValue(100), nextRevision());
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

    @Nested
    class StreamLifecycle {

        @Test
        void closeReleasesResources() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), value(i), nextRevision());
                }
                tree.flush();

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    assertTrue(stream.findFirst().isPresent());
                }
            }
        }

        @Test
        void multipleStreamsOnSameTree() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 20; i++) {
                    tree.put(key(i), value(i), nextRevision());
                }
                tree.flush();

                try (Stream<Entry> stream1 = tree.scan(null, null);
                     Stream<Entry> stream2 = tree.scan(null, null);
                     Stream<Entry> stream3 = tree.scan(null, null)) {

                    List<Entry> entries1 = stream1.toList();
                    List<Entry> entries2 = stream2.toList();
                    List<Entry> entries3 = stream3.toList();

                    assertEquals(20, entries1.size());
                    assertEquals(20, entries2.size());
                    assertEquals(20, entries3.size());
                }
            }
        }

        @Test
        void streamSurvivesCompaction() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 30; i++) {
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                    }
                    tree.flush();
                }

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    Thread.sleep(1000);

                    List<Entry> entries = stream.toList();
                    assertEquals(30, entries.size());
                    for (Entry e : entries) {
                        assertNotNull(e.key());
                        assertNotNull(e.value());
                    }
                }
            }
        }

        @Test
        void closeAfterPartialConsumption() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(i), value(i), nextRevision());
                }
                tree.flush();

                try (Stream<Entry> stream = tree.scan(null, null)) {
                    List<Entry> first10 = stream.limit(10).toList();
                    assertEquals(10, first10.size());
                }
            }
        }
    }
}
