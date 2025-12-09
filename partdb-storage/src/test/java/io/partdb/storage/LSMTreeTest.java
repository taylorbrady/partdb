package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;
import io.partdb.storage.compaction.CompactionConfig;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.manifest.Manifest;
import io.partdb.storage.manifest.SSTableInfo;
import io.partdb.storage.memtable.MemtableConfig;
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

    private static ByteArray key(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray key(String s) {
        return ByteArray.copyOf(s.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteArray value(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray value(String s) {
        return ByteArray.copyOf(s.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteArray largeValue(int size) {
        return ByteArray.copyOf(new byte[size]);
    }

    private static LSMConfig smallMemtableConfig(int sizeBytes) {
        return new LSMConfig(
            new MemtableConfig(sizeBytes),
            SSTableConfig.defaults(),
            CompactionConfig.defaults(),
            LeveledCompactionConfig.defaults()
        );
    }

    private final AtomicLong timestampCounter = new AtomicLong(0);

    private Timestamp nextTimestamp() {
        return new Timestamp(timestampCounter.incrementAndGet());
    }

    @Nested
    class BasicOperations {

        @Test
        void putAndGet() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result = snap.get(key(1));

                    assertTrue(result.isPresent());
                    assertEquals(value(10), result.get().value());
                }
            }
        }

        @Test
        void getNonExistentKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result = snap.get(key(99));
                    assertTrue(result.isEmpty());
                }
            }
        }

        @Test
        void deleteKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.delete(key(1), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result = snap.get(key(1));
                    assertTrue(result.isEmpty());
                }
            }
        }

        @Test
        void deleteNonExistentKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.delete(key(1), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result = snap.get(key(1));
                    assertTrue(result.isEmpty());
                }
            }
        }

        @Test
        void putOverwritesPreviousValue() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(1), value(20), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result = snap.get(key(1));

                    assertTrue(result.isPresent());
                    assertEquals(value(20), result.get().value());
                }
            }
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());
                tree.put(key(3), value(30), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    List<KeyValue> entries = stream.toList();

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
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());
                tree.put(key(3), value(30), nextTimestamp());
                tree.put(key(4), value(40), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(key(2), key(4))) {
                    List<KeyValue> entries = stream.toList();

                    assertEquals(2, entries.size());
                    assertEquals(key(2), entries.get(0).key());
                    assertEquals(key(3), entries.get(1).key());
                }
            }
        }

        @Test
        void excludesDeletedKeys() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());
                tree.delete(key(2), nextTimestamp());
                tree.put(key(3), value(30), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    List<KeyValue> entries = stream.toList();

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
                    tree.put(key(i), largeValue(100), nextTimestamp());
                }

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    List<KeyValue> entries = stream.toList();

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
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), largeValue(100), nextTimestamp());
                tree.put(key(1), value(20), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    List<KeyValue> entries = stream.toList();

                    Optional<KeyValue> keyEntry = entries.stream()
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
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());

                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertTrue(snap.get(key(2)).isPresent());
                }
            }
        }

        @Test
        void automaticOnMemtableSize() {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), largeValue(200), nextTimestamp());
                }

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    for (int i = 0; i < 10; i++) {
                        assertTrue(snap.get(key(i)).isPresent());
                    }
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
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());
                tree.flush();
            }

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result1 = snap.get(key(1));
                    Optional<KeyValue> result2 = snap.get(key(2));

                    assertTrue(result1.isPresent());
                    assertEquals(value(10), result1.get().value());

                    assertTrue(result2.isPresent());
                    assertEquals(value(20), result2.get().value());
                }
            }
        }

        @Test
        void readPathPriority() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();
                tree.put(key(1), value(20), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    Optional<KeyValue> result = snap.get(key(1));

                    assertTrue(result.isPresent());
                    assertEquals(value(20), result.get().value());
                }
            }
        }

        @Test
        void multipleSSTables() {
            LSMConfig config = smallMemtableConfig(512);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(i), largeValue(100), nextTimestamp());
                }

                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    for (int i = 0; i < 30; i++) {
                        assertTrue(snap.get(key(i)).isPresent());
                    }
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
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextTimestamp());
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
                        tree.put(key(String.format("key-%02d", i)), value("v" + version + "-" + i), nextTimestamp());
                    }
                    tree.flush();
                }

                Thread.sleep(1000);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    for (int i = 0; i < 20; i++) {
                        Optional<KeyValue> result = snap.get(key(String.format("key-%02d", i)));
                        assertTrue(result.isPresent());
                        assertTrue(new String(result.get().value().toByteArray()).startsWith("v4"));
                    }
                }
            }
        }

        @Test
        void tombstonesResultInEmptyGet() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextTimestamp());
                }
                tree.flush();

                for (int i = 0; i < 50; i++) {
                    tree.delete(key(String.format("key-%03d", i)), nextTimestamp());
                }
                tree.flush();

                Thread.sleep(500);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    for (int i = 0; i < 50; i++) {
                        assertTrue(snap.get(key(String.format("key-%03d", i))).isEmpty());
                    }
                }
            }
        }

        @Test
        void levelSizeRespected() throws Exception {
            LSMConfig config = smallMemtableConfig(2048);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 20; batch++) {
                    for (int i = 0; i < 100; i++) {
                        tree.put(key(String.format("key-%05d", batch * 100 + i)), largeValue(100), nextTimestamp());
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
                    tree.put(key(String.format("key-%02d", i)), value(val), nextTimestamp());

                    if (i % 10 == 9) {
                        tree.flush();
                    }
                }

                Thread.sleep(1000);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    for (int i = 0; i < 30; i++) {
                        Optional<KeyValue> result = snap.get(key(String.format("key-%02d", i)));
                        assertTrue(result.isPresent());
                        assertEquals(expectedValues.get(i), new String(result.get().value().toByteArray()));
                    }
                }
            }
        }

        @Test
        void manifestConsistency() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 80; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextTimestamp());
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

            List<ByteArray> keys = new ArrayList<>();
            List<ByteArray> values = new ArrayList<>();

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 60; i++) {
                    ByteArray k = key(String.format("key-%03d", i));
                    ByteArray v = value("value-" + i);
                    keys.add(k);
                    values.add(v);
                    tree.put(k, v, nextTimestamp());
                }
                tree.flush();
                Thread.sleep(500);
            }

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    for (int i = 0; i < keys.size(); i++) {
                        Optional<KeyValue> result = snap.get(keys.get(i));
                        assertTrue(result.isPresent());
                        assertEquals(values.get(i), result.get().value());
                    }
                }
            }
        }

        @Test
        void scanAfterCompaction() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i), nextTimestamp());
                }
                tree.flush();

                Thread.sleep(500);

                ByteArray startKey = key("key-020");
                ByteArray endKey = key("key-030");

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(startKey, endKey)) {
                    List<KeyValue> entries = stream.toList();

                    assertEquals(10, entries.size());
                    for (KeyValue kv : entries) {
                        assertTrue(kv.key().compareTo(startKey) >= 0);
                        assertTrue(kv.key().compareTo(endKey) < 0);
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
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();
                tree.restoreFromCheckpoint(checkpoint);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertEquals(value(10), snap.get(key(1)).get().value());
                    assertTrue(snap.get(key(2)).isPresent());
                    assertEquals(value(20), snap.get(key(2)).get().value());
                }
            }
        }

        @Test
        void restoresToPreviousState() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();

                tree.put(key(2), value(20), nextTimestamp());
                tree.put(key(3), value(30), nextTimestamp());
                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(2)).isPresent());
                    assertTrue(snap.get(key(3)).isPresent());
                }

                tree.restoreFromCheckpoint(checkpoint);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertEquals(value(10), snap.get(key(1)).get().value());
                    assertTrue(snap.get(key(2)).isEmpty());
                    assertTrue(snap.get(key(3)).isEmpty());
                }
            }
        }

        @Test
        void capturesMultipleSSTables() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();

                tree.put(key(2), value(20), nextTimestamp());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();
                assertEquals(2, tree.manifest().sstables().size());

                tree.put(key(3), value(30), nextTimestamp());
                tree.flush();

                tree.restoreFromCheckpoint(checkpoint);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertTrue(snap.get(key(2)).isPresent());
                    assertTrue(snap.get(key(3)).isEmpty());
                }
                assertEquals(2, tree.manifest().sstables().size());
            }
        }

        @Test
        void clearsMemtableOnRestore() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();

                tree.put(key(2), value(20), nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(2)).isPresent());
                }

                tree.restoreFromCheckpoint(checkpoint);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertTrue(snap.get(key(2)).isEmpty());
                }
            }
        }

        @Test
        void manifestStateRestored() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();

                byte[] checkpoint = tree.checkpoint();
                long originalNextId = tree.manifest().nextSSTableId();
                int originalSSTableCount = tree.manifest().sstables().size();

                tree.put(key(2), value(20), nextTimestamp());
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
                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();
                byte[] checkpoint1 = tree.checkpoint();

                tree.put(key(2), value(20), nextTimestamp());
                tree.flush();
                byte[] checkpoint2 = tree.checkpoint();

                tree.put(key(3), value(30), nextTimestamp());
                tree.flush();

                tree.restoreFromCheckpoint(checkpoint1);
                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertTrue(snap.get(key(2)).isEmpty());
                    assertTrue(snap.get(key(3)).isEmpty());
                }

                tree.restoreFromCheckpoint(checkpoint2);
                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertTrue(snap.get(key(2)).isPresent());
                    assertTrue(snap.get(key(3)).isEmpty());
                }
            }
        }

        @Test
        void emptyTree() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                byte[] checkpoint = tree.checkpoint();

                tree.put(key(1), value(10), nextTimestamp());
                tree.flush();
                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                }

                tree.restoreFromCheckpoint(checkpoint);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isEmpty());
                }
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
                    tree.put(key(i), value(i), nextTimestamp());
                }
                tree.flush();

                int threadCount = 10;
                int readsPerThread = 100;
                AtomicBoolean failed = new AtomicBoolean(false);

                try (var executor = Executors.newFixedThreadPool(threadCount)) {
                    for (int t = 0; t < threadCount; t++) {
                        executor.submit(() -> {
                            try (var snap = tree.snapshot(Timestamp.MAX)) {
                                for (int i = 0; i < readsPerThread; i++) {
                                    int keyIndex = i % 100;
                                    Optional<KeyValue> result = snap.get(key(keyIndex));
                                    if (result.isEmpty() || !result.get().value().equals(value(keyIndex))) {
                                        failed.set(true);
                                    }
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
                                tree.put(key(keyIndex & 0xFF), value(keyIndex), nextTimestamp());
                            }
                        });
                    }

                    executor.shutdown();
                    assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS),
                        "Concurrent write tasks did not complete in time");
                }

                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    List<KeyValue> entries = stream.toList();
                    assertFalse(entries.isEmpty());
                }
            }
        }

        @Test
        void concurrentReadsAndWrites() throws Exception {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i), nextTimestamp());
                }

                int writerCount = 5;
                int readerCount = 5;
                AtomicBoolean failed = new AtomicBoolean(false);

                try (var executor = Executors.newFixedThreadPool(writerCount + readerCount)) {
                    for (int t = 0; t < writerCount; t++) {
                        int threadId = t;
                        executor.submit(() -> {
                            for (int i = 0; i < 100; i++) {
                                tree.put(key((50 + threadId * 100 + i) & 0xFF), value(i), nextTimestamp());
                            }
                        });
                    }

                    for (int t = 0; t < readerCount; t++) {
                        executor.submit(() -> {
                            try (var snap = tree.snapshot(Timestamp.MAX)) {
                                for (int i = 0; i < 100; i++) {
                                    snap.get(key(i % 50));
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
                    tree.put(key(i), value(i), nextTimestamp());
                }

                int readerCount = 5;
                CountDownLatch startLatch = new CountDownLatch(1);
                AtomicBoolean failed = new AtomicBoolean(false);

                try (var executor = Executors.newFixedThreadPool(readerCount + 1)) {
                    for (int t = 0; t < readerCount; t++) {
                        executor.submit(() -> {
                            try {
                                startLatch.await();
                                try (var snap = tree.snapshot(Timestamp.MAX)) {
                                    for (int i = 0; i < 200; i++) {
                                        snap.get(key(i % 50));
                                    }
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
                                    tree.put(key((100 + round * 20 + i) & 0xFF), largeValue(100), nextTimestamp());
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
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextTimestamp());
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
                                    try (var snap = tree.snapshot(Timestamp.MAX)) {
                                        for (int i = 0; i < 50; i++) {
                                            Optional<KeyValue> result = snap.get(key(String.format("key-%03d", i)));
                                            if (result.isEmpty()) {
                                                failed.set(true);
                                            }
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
                    tree.put(key(i), value(i), nextTimestamp());
                }

                CountDownLatch startLatch = new CountDownLatch(1);
                AtomicBoolean failed = new AtomicBoolean(false);

                try (var executor = Executors.newFixedThreadPool(2)) {
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            for (int round = 0; round < 10; round++) {
                                try (var snap = tree.snapshot(Timestamp.MAX);
                                     Stream<KeyValue> stream = snap.scan(null, null)) {
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
                                    tree.put(key((100 + round * 20 + i) & 0xFF), largeValue(100), nextTimestamp());
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
                    tree.put(key(i), value(i), nextTimestamp());
                }
                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    assertTrue(stream.findFirst().isPresent());
                }
            }
        }

        @Test
        void multipleStreamsOnSameTree() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 20; i++) {
                    tree.put(key(i), value(i), nextTimestamp());
                }
                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream1 = snap.scan(null, null);
                     Stream<KeyValue> stream2 = snap.scan(null, null);
                     Stream<KeyValue> stream3 = snap.scan(null, null)) {

                    List<KeyValue> entries1 = stream1.toList();
                    List<KeyValue> entries2 = stream2.toList();
                    List<KeyValue> entries3 = stream3.toList();

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
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextTimestamp());
                    }
                    tree.flush();
                }

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    Thread.sleep(1000);

                    List<KeyValue> entries = stream.toList();
                    assertEquals(30, entries.size());
                    for (KeyValue kv : entries) {
                        assertNotNull(kv.key());
                        assertNotNull(kv.value());
                    }
                }
            }
        }

        @Test
        void streamSeesConsistentSnapshot() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), value(i), nextTimestamp());
                }
                tree.flush();

                Timestamp snapshotTs = new Timestamp(timestampCounter.get());
                try (var snap = tree.snapshot(snapshotTs);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    for (int i = 10; i < 20; i++) {
                        tree.put(key(i), value(i), nextTimestamp());
                    }
                    tree.flush();

                    List<KeyValue> entries = stream.toList();

                    assertEquals(10, entries.size());
                    for (int i = 0; i < 10; i++) {
                        assertEquals(key(i), entries.get(i).key());
                    }
                }
            }
        }

        @Test
        void unclosedStreamPreventsReaderClose() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("initial-" + i), nextTimestamp());
                }
                tree.flush();

                Timestamp snapshotTs = new Timestamp(timestampCounter.get());
                var snap = tree.snapshot(snapshotTs);
                Stream<KeyValue> stream = snap.scan(null, null);

                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 30; i++) {
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextTimestamp());
                    }
                    tree.flush();
                }
                Thread.sleep(1000);

                List<KeyValue> entries = stream.toList();
                assertEquals(30, entries.size());
                for (KeyValue kv : entries) {
                    String valueStr = new String(kv.value().toByteArray());
                    assertTrue(valueStr.startsWith("initial-"));
                }

                stream.close();
                snap.close();
            }
        }

        @Test
        void closeAfterPartialConsumption() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(i), value(i), nextTimestamp());
                }
                tree.flush();

                try (var snap = tree.snapshot(Timestamp.MAX);
                     Stream<KeyValue> stream = snap.scan(null, null)) {
                    List<KeyValue> first10 = stream.limit(10).toList();
                    assertEquals(10, first10.size());
                }
            }
        }
    }

    @Nested
    class WriteBatchTests {

        @Test
        void writeBatchAppliesAtomically() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                Timestamp ts = nextTimestamp();

                WriteBatch batch = WriteBatch.builder()
                    .put(key(1), value(10))
                    .put(key(2), value(20))
                    .put(key(3), value(30))
                    .build();

                tree.write(batch, ts);

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                    assertEquals(value(10), snap.get(key(1)).get().value());
                    assertTrue(snap.get(key(2)).isPresent());
                    assertEquals(value(20), snap.get(key(2)).get().value());
                    assertTrue(snap.get(key(3)).isPresent());
                    assertEquals(value(30), snap.get(key(3)).get().value());
                }
            }
        }

        @Test
        void writeBatchWithPutsAndDeletes() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());

                WriteBatch batch = WriteBatch.builder()
                    .delete(key(1))
                    .put(key(3), value(30))
                    .build();

                tree.write(batch, nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isEmpty());
                    assertTrue(snap.get(key(2)).isPresent());
                    assertTrue(snap.get(key(3)).isPresent());
                }
            }
        }

        @Test
        void emptyWriteBatchIsNoop() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());

                WriteBatch batch = WriteBatch.builder().build();
                assertTrue(batch.isEmpty());
                assertEquals(0, batch.size());

                tree.write(batch, nextTimestamp());

                try (var snap = tree.snapshot(Timestamp.MAX)) {
                    assertTrue(snap.get(key(1)).isPresent());
                }
            }
        }

        @Test
        void writeBatchFluentApi() {
            WriteBatch batch = WriteBatch.builder()
                .put(key(1), value(10))
                .put(key(2), value(20))
                .delete(key(3))
                .put(key(4), value(40))
                .build();

            assertEquals(4, batch.size());
            assertFalse(batch.isEmpty());
        }
    }

    @Nested
    class SnapshotTests {

        @Test
        void snapshotSeesConsistentState() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                tree.put(key(2), value(20), nextTimestamp());

                Timestamp snapTs = new Timestamp(timestampCounter.get());

                try (var snap = tree.snapshot(snapTs)) {
                    tree.put(key(3), value(30), nextTimestamp());
                    tree.put(key(1), value(100), nextTimestamp());

                    assertTrue(snap.get(key(1)).isPresent());
                    assertEquals(value(10), snap.get(key(1)).get().value());
                    assertTrue(snap.get(key(2)).isPresent());
                    assertTrue(snap.get(key(3)).isEmpty());
                }
            }
        }

        @Test
        void closedSnapshotThrows() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());

                var snap = tree.snapshot(Timestamp.MAX);
                snap.close();

                IllegalStateException ex = assertThrows(IllegalStateException.class, () -> snap.get(key(1)));
                assertTrue(ex.getMessage().contains("closed"));
            }
        }

        @Test
        void multipleSnapshotsIndependent() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.defaults())) {
                tree.put(key(1), value(10), nextTimestamp());
                Timestamp ts1 = new Timestamp(timestampCounter.get());

                tree.put(key(2), value(20), nextTimestamp());
                Timestamp ts2 = new Timestamp(timestampCounter.get());

                tree.put(key(3), value(30), nextTimestamp());

                try (var snap1 = tree.snapshot(ts1);
                     var snap2 = tree.snapshot(ts2);
                     var snap3 = tree.snapshot(Timestamp.MAX)) {

                    assertTrue(snap1.get(key(1)).isPresent());
                    assertTrue(snap1.get(key(2)).isEmpty());
                    assertTrue(snap1.get(key(3)).isEmpty());

                    assertTrue(snap2.get(key(1)).isPresent());
                    assertTrue(snap2.get(key(2)).isPresent());
                    assertTrue(snap2.get(key(3)).isEmpty());

                    assertTrue(snap3.get(key(1)).isPresent());
                    assertTrue(snap3.get(key(2)).isPresent());
                    assertTrue(snap3.get(key(3)).isPresent());
                }
            }
        }
    }
}
