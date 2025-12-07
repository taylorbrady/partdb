package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.KeyValue;
import io.partdb.storage.compaction.LeveledCompactionConfig;
import io.partdb.storage.compaction.Manifest;
import io.partdb.storage.compaction.SSTableMetadata;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class LSMTreeTest {

    @TempDir
    Path tempDir;

    private static ByteArray key(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray key(String s) {
        return ByteArray.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteArray value(int i) {
        return ByteArray.of((byte) i);
    }

    private static ByteArray value(String s) {
        return ByteArray.wrap(s.getBytes(StandardCharsets.UTF_8));
    }

    private static ByteArray largeValue(int size) {
        return ByteArray.wrap(new byte[size]);
    }

    private static LSMConfig smallMemtableConfig(int sizeBytes) {
        return new LSMConfig(new MemtableConfig(sizeBytes), SSTableConfig.create());
    }

    private List<KeyValue> collectEntries(CloseableIterator<KeyValue> iterator) {
        List<KeyValue> entries = new ArrayList<>();
        while (iterator.hasNext()) {
            entries.add(iterator.next());
        }
        return entries;
    }

    @Nested
    class BasicOperations {

        @Test
        void putAndGet() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));

                Optional<ByteArray> result = tree.get(key(1));

                assertThat(result).isPresent();
                assertThat(result.get()).isEqualTo(value(10));
            }
        }

        @Test
        void getNonExistentKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                Optional<ByteArray> result = tree.get(key(99));
                assertThat(result).isEmpty();
            }
        }

        @Test
        void deleteKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.delete(key(1));

                Optional<ByteArray> result = tree.get(key(1));
                assertThat(result).isEmpty();
            }
        }

        @Test
        void deleteNonExistentKey() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.delete(key(1));

                Optional<ByteArray> result = tree.get(key(1));
                assertThat(result).isEmpty();
            }
        }

        @Test
        void putOverwritesPreviousValue() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.put(key(1), value(20));

                Optional<ByteArray> result = tree.get(key(1));

                assertThat(result).isPresent();
                assertThat(result.get()).isEqualTo(value(20));
            }
        }
    }

    @Nested
    class Scan {

        @Test
        void entireRange() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.put(key(2), value(20));
                tree.put(key(3), value(30));

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    List<KeyValue> entries = collectEntries(it);

                    assertThat(entries).hasSize(3);
                    assertThat(entries.get(0).key()).isEqualTo(key(1));
                    assertThat(entries.get(1).key()).isEqualTo(key(2));
                    assertThat(entries.get(2).key()).isEqualTo(key(3));
                }
            }
        }

        @Test
        void withBounds() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.put(key(2), value(20));
                tree.put(key(3), value(30));
                tree.put(key(4), value(40));

                try (CloseableIterator<KeyValue> it = tree.scan(key(2), key(4))) {
                    List<KeyValue> entries = collectEntries(it);

                    assertThat(entries).hasSize(2);
                    assertThat(entries.get(0).key()).isEqualTo(key(2));
                    assertThat(entries.get(1).key()).isEqualTo(key(3));
                }
            }
        }

        @Test
        void excludesDeletedKeys() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.put(key(2), value(20));
                tree.delete(key(2));
                tree.put(key(3), value(30));

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    List<KeyValue> entries = collectEntries(it);

                    assertThat(entries).hasSize(2);
                    assertThat(entries.get(0).key()).isEqualTo(key(1));
                    assertThat(entries.get(1).key()).isEqualTo(key(3));
                }
            }
        }

        @Test
        void mergesFromMultipleSources() {
            LSMConfig config = smallMemtableConfig(512);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 20; i++) {
                    tree.put(key(i), largeValue(100));
                }

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    List<KeyValue> entries = collectEntries(it);

                    assertThat(entries).hasSize(20);
                    for (int i = 0; i < 20; i++) {
                        assertThat(entries.get(i).key()).isEqualTo(key(i));
                    }
                }
            }
        }

        @Test
        void usesLatestValueForDuplicateKeys() {
            LSMConfig config = smallMemtableConfig(256);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                tree.put(key(1), value(10));
                tree.put(key(2), largeValue(100));
                tree.put(key(1), value(20));

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    List<KeyValue> entries = collectEntries(it);

                    Optional<KeyValue> keyEntry = entries.stream()
                        .filter(e -> e.key().equals(key(1)))
                        .findFirst();

                    assertThat(keyEntry).isPresent();
                    assertThat(keyEntry.get().value()).isEqualTo(value(20));
                }
            }
        }
    }

    @Nested
    class Flush {

        @Test
        void manualFlush() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.put(key(2), value(20));

                tree.flush();

                assertThat(tree.get(key(1))).isPresent();
                assertThat(tree.get(key(2))).isPresent();
            }
        }

        @Test
        void automaticOnMemtableSize() {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), largeValue(200));
                }

                for (int i = 0; i < 10; i++) {
                    assertThat(tree.get(key(i))).isPresent();
                }
            }
        }
    }

    @Nested
    class Recovery {

        @Test
        void fromSSTables() {
            LSMConfig config = LSMConfig.create();

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                tree.put(key(1), value(10));
                tree.put(key(2), value(20));
                tree.flush();
            }

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                Optional<ByteArray> result1 = tree.get(key(1));
                Optional<ByteArray> result2 = tree.get(key(2));

                assertThat(result1).isPresent();
                assertThat(result1.get()).isEqualTo(value(10));

                assertThat(result2).isPresent();
                assertThat(result2.get()).isEqualTo(value(20));
            }
        }

        @Test
        void readPathPriority() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.flush();
                tree.put(key(1), value(20));

                Optional<ByteArray> result = tree.get(key(1));

                assertThat(result).isPresent();
                assertThat(result.get()).isEqualTo(value(20));
            }
        }

        @Test
        void multipleSSTables() {
            LSMConfig config = smallMemtableConfig(512);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(i), largeValue(100));
                }

                tree.flush();

                for (int i = 0; i < 30; i++) {
                    assertThat(tree.get(key(i))).isPresent();
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
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i));
                }

                tree.flush();
                Thread.sleep(500);

                Manifest manifest = tree.manifest();
                List<SSTableMetadata> l0Files = manifest.level(0);
                List<SSTableMetadata> l1Files = manifest.level(1);

                assertThat(l0Files.size()).isLessThan(4);
                assertThat(l1Files.size()).isGreaterThan(0);
            }
        }

        @Test
        void mergesOverlappingKeys() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int version = 0; version < 5; version++) {
                    for (int i = 0; i < 20; i++) {
                        tree.put(key(String.format("key-%02d", i)), value("v" + version + "-" + i));
                    }
                    tree.flush();
                }

                Thread.sleep(1000);

                for (int i = 0; i < 20; i++) {
                    Optional<ByteArray> result = tree.get(key(String.format("key-%02d", i)));
                    assertThat(result).isPresent();
                    assertThat(new String(result.get().toByteArray())).startsWith("v4");
                }
            }
        }

        @Test
        void tombstonesResultInEmptyGet() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i));
                }
                tree.flush();

                for (int i = 0; i < 50; i++) {
                    tree.delete(key(String.format("key-%03d", i)));
                }
                tree.flush();

                Thread.sleep(500);

                for (int i = 0; i < 50; i++) {
                    assertThat(tree.get(key(String.format("key-%03d", i)))).isEmpty();
                }
            }
        }

        @Test
        void levelSizeRespected() throws Exception {
            LSMConfig config = smallMemtableConfig(2048);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 20; batch++) {
                    for (int i = 0; i < 100; i++) {
                        tree.put(key(String.format("key-%05d", batch * 100 + i)), largeValue(100));
                    }
                    tree.flush();
                }

                Thread.sleep(2000);

                Manifest manifest = tree.manifest();
                LeveledCompactionConfig compactionConfig = LeveledCompactionConfig.create();

                for (int level = 1; level < manifest.maxLevel(); level++) {
                    long levelSize = manifest.levelSize(level);
                    long maxSize = compactionConfig.maxBytesForLevel(level);

                    assertThat(levelSize).isLessThanOrEqualTo(maxSize * 2);
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
                    tree.put(key(String.format("key-%02d", i)), value(val));

                    if (i % 10 == 9) {
                        tree.flush();
                    }
                }

                Thread.sleep(1000);

                for (int i = 0; i < 30; i++) {
                    Optional<ByteArray> result = tree.get(key(String.format("key-%02d", i)));
                    assertThat(result).isPresent();
                    assertThat(new String(result.get().toByteArray())).isEqualTo(expectedValues.get(i));
                }
            }
        }

        @Test
        void manifestConsistency() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 80; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i));
                }
                tree.flush();

                Thread.sleep(500);

                Manifest manifest = tree.manifest();

                long totalEntries = 0;
                for (SSTableMetadata meta : manifest.sstables()) {
                    totalEntries += meta.entryCount();
                    assertThat(meta.id()).isGreaterThan(0);
                    assertThat(meta.level()).isGreaterThanOrEqualTo(0);
                    assertThat(meta.fileSizeBytes()).isGreaterThan(0);
                    assertThat(meta.smallestKey()).isNotNull();
                    assertThat(meta.largestKey()).isNotNull();
                }

                assertThat(totalEntries).isGreaterThanOrEqualTo(80);
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
                    tree.put(k, v);
                }
                tree.flush();
                Thread.sleep(500);
            }

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < keys.size(); i++) {
                    Optional<ByteArray> result = tree.get(keys.get(i));
                    assertThat(result).isPresent();
                    assertThat(result.get()).isEqualTo(values.get(i));
                }
            }
        }

        @Test
        void scanAfterCompaction() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("value-" + i));
                }
                tree.flush();

                Thread.sleep(500);

                ByteArray startKey = key("key-020");
                ByteArray endKey = key("key-030");

                try (CloseableIterator<KeyValue> iterator = tree.scan(startKey, endKey)) {
                    int count = 0;
                    while (iterator.hasNext()) {
                        KeyValue kv = iterator.next();
                        count++;
                        assertThat(kv.key().compareTo(startKey)).isGreaterThanOrEqualTo(0);
                        assertThat(kv.key().compareTo(endKey)).isLessThan(0);
                    }

                    assertThat(count).isEqualTo(10);
                }
            }
        }

        @Test
        void emptyManifestLoad() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                Manifest manifest = tree.manifest();
                assertThat(manifest).isNotNull();
                assertThat(manifest.sstables()).isEmpty();
            }
        }
    }

    @Nested
    class Snapshot {

        @Test
        void roundtrip() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.put(key(2), value(20));
                tree.flush();

                byte[] snapshot = tree.snapshot();
                tree.restore(snapshot);

                assertThat(tree.get(key(1))).isPresent().contains(value(10));
                assertThat(tree.get(key(2))).isPresent().contains(value(20));
            }
        }

        @Test
        void restoresToPreviousState() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.flush();

                byte[] snapshot = tree.snapshot();

                tree.put(key(2), value(20));
                tree.put(key(3), value(30));
                tree.flush();

                assertThat(tree.get(key(2))).isPresent();
                assertThat(tree.get(key(3))).isPresent();

                tree.restore(snapshot);

                assertThat(tree.get(key(1))).isPresent().contains(value(10));
                assertThat(tree.get(key(2))).isEmpty();
                assertThat(tree.get(key(3))).isEmpty();
            }
        }

        @Test
        void capturesMultipleSSTables() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.flush();

                tree.put(key(2), value(20));
                tree.flush();

                byte[] snapshot = tree.snapshot();
                assertThat(tree.manifest().sstables()).hasSize(2);

                tree.put(key(3), value(30));
                tree.flush();

                tree.restore(snapshot);

                assertThat(tree.get(key(1))).isPresent();
                assertThat(tree.get(key(2))).isPresent();
                assertThat(tree.get(key(3))).isEmpty();
                assertThat(tree.manifest().sstables()).hasSize(2);
            }
        }

        @Test
        void clearsMemtableOnRestore() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.flush();

                byte[] snapshot = tree.snapshot();

                tree.put(key(2), value(20));

                assertThat(tree.get(key(2))).isPresent();

                tree.restore(snapshot);

                assertThat(tree.get(key(1))).isPresent();
                assertThat(tree.get(key(2))).isEmpty();
            }
        }

        @Test
        void manifestStateRestored() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.flush();

                byte[] snapshot = tree.snapshot();
                long originalNextId = tree.manifest().nextSSTableId();
                int originalSSTableCount = tree.manifest().sstables().size();

                tree.put(key(2), value(20));
                tree.flush();

                assertThat(tree.manifest().nextSSTableId()).isGreaterThan(originalNextId);

                tree.restore(snapshot);

                assertThat(tree.manifest().nextSSTableId()).isEqualTo(originalNextId);
                assertThat(tree.manifest().sstables()).hasSize(originalSSTableCount);
            }
        }

        @Test
        void multipleSnapshots() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                tree.put(key(1), value(10));
                tree.flush();
                byte[] snapshot1 = tree.snapshot();

                tree.put(key(2), value(20));
                tree.flush();
                byte[] snapshot2 = tree.snapshot();

                tree.put(key(3), value(30));
                tree.flush();

                tree.restore(snapshot1);
                assertThat(tree.get(key(1))).isPresent();
                assertThat(tree.get(key(2))).isEmpty();
                assertThat(tree.get(key(3))).isEmpty();

                tree.restore(snapshot2);
                assertThat(tree.get(key(1))).isPresent();
                assertThat(tree.get(key(2))).isPresent();
                assertThat(tree.get(key(3))).isEmpty();
            }
        }

        @Test
        void emptyTree() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                byte[] snapshot = tree.snapshot();

                tree.put(key(1), value(10));
                tree.flush();
                assertThat(tree.get(key(1))).isPresent();

                tree.restore(snapshot);

                assertThat(tree.get(key(1))).isEmpty();
                assertThat(tree.manifest().sstables()).isEmpty();
            }
        }
    }

    @Nested
    class Concurrency {

        @Test
        void concurrentReads() throws Exception {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(i), value(i));
                }
                tree.flush();

                int threadCount = 10;
                int readsPerThread = 100;
                ExecutorService executor = Executors.newFixedThreadPool(threadCount);
                CountDownLatch latch = new CountDownLatch(threadCount);
                AtomicBoolean failed = new AtomicBoolean(false);

                for (int t = 0; t < threadCount; t++) {
                    executor.submit(() -> {
                        try {
                            for (int i = 0; i < readsPerThread; i++) {
                                int keyIndex = i % 100;
                                Optional<ByteArray> result = tree.get(key(keyIndex));
                                if (result.isEmpty() || !result.get().equals(value(keyIndex))) {
                                    failed.set(true);
                                }
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
                }

                latch.await(10, TimeUnit.SECONDS);
                executor.shutdown();

                assertThat(failed.get()).isFalse();
            }
        }

        @Test
        void concurrentWrites() throws Exception {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                int threadCount = 10;
                int writesPerThread = 100;
                ExecutorService executor = Executors.newFixedThreadPool(threadCount);
                CountDownLatch latch = new CountDownLatch(threadCount);

                for (int t = 0; t < threadCount; t++) {
                    int threadId = t;
                    executor.submit(() -> {
                        try {
                            for (int i = 0; i < writesPerThread; i++) {
                                int keyIndex = threadId * writesPerThread + i;
                                tree.put(key(keyIndex & 0xFF), value(keyIndex));
                            }
                        } finally {
                            latch.countDown();
                        }
                    });
                }

                latch.await(10, TimeUnit.SECONDS);
                executor.shutdown();

                tree.flush();

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    List<KeyValue> entries = collectEntries(it);
                    assertThat(entries).isNotEmpty();
                }
            }
        }

        @Test
        void concurrentReadsAndWrites() throws Exception {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i));
                }

                int writerCount = 5;
                int readerCount = 5;
                ExecutorService executor = Executors.newFixedThreadPool(writerCount + readerCount);
                CountDownLatch latch = new CountDownLatch(writerCount + readerCount);
                AtomicBoolean failed = new AtomicBoolean(false);

                for (int t = 0; t < writerCount; t++) {
                    int threadId = t;
                    executor.submit(() -> {
                        try {
                            for (int i = 0; i < 100; i++) {
                                tree.put(key((50 + threadId * 100 + i) & 0xFF), value(i));
                            }
                        } finally {
                            latch.countDown();
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
                        } finally {
                            latch.countDown();
                        }
                    });
                }

                latch.await(10, TimeUnit.SECONDS);
                executor.shutdown();

                assertThat(failed.get()).isFalse();
            }
        }

        @Test
        void readsWhileFlushing() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i));
                }

                int readerCount = 5;
                ExecutorService executor = Executors.newFixedThreadPool(readerCount + 1);
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch doneLatch = new CountDownLatch(readerCount + 1);
                AtomicBoolean failed = new AtomicBoolean(false);

                for (int t = 0; t < readerCount; t++) {
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            for (int i = 0; i < 200; i++) {
                                tree.get(key(i % 50));
                            }
                        } catch (Exception e) {
                            failed.set(true);
                        } finally {
                            doneLatch.countDown();
                        }
                    });
                }

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int round = 0; round < 5; round++) {
                            for (int i = 0; i < 20; i++) {
                                tree.put(key((100 + round * 20 + i) & 0xFF), largeValue(100));
                            }
                            tree.flush();
                        }
                    } catch (Exception e) {
                        failed.set(true);
                    } finally {
                        doneLatch.countDown();
                    }
                });

                startLatch.countDown();
                doneLatch.await(30, TimeUnit.SECONDS);
                executor.shutdown();

                assertThat(failed.get()).isFalse();
            }
        }

        @Test
        void readsWhileCompacting() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 50; i++) {
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i));
                    }
                    tree.flush();
                }

                int readerCount = 5;
                ExecutorService executor = Executors.newFixedThreadPool(readerCount);
                CountDownLatch latch = new CountDownLatch(readerCount);
                AtomicBoolean failed = new AtomicBoolean(false);

                for (int t = 0; t < readerCount; t++) {
                    executor.submit(() -> {
                        try {
                            for (int round = 0; round < 50; round++) {
                                for (int i = 0; i < 50; i++) {
                                    Optional<ByteArray> result = tree.get(key(String.format("key-%03d", i)));
                                    if (result.isEmpty()) {
                                        failed.set(true);
                                    }
                                }
                                Thread.sleep(10);
                            }
                        } catch (Exception e) {
                            failed.set(true);
                        } finally {
                            latch.countDown();
                        }
                    });
                }

                latch.await(30, TimeUnit.SECONDS);
                executor.shutdown();

                assertThat(failed.get()).isFalse();
            }
        }

        @Test
        void scanWhileFlushing() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 50; i++) {
                    tree.put(key(i), value(i));
                }

                ExecutorService executor = Executors.newFixedThreadPool(2);
                CountDownLatch startLatch = new CountDownLatch(1);
                CountDownLatch doneLatch = new CountDownLatch(2);
                AtomicBoolean failed = new AtomicBoolean(false);

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int round = 0; round < 10; round++) {
                            try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                                while (it.hasNext()) {
                                    it.next();
                                }
                            }
                        }
                    } catch (Exception e) {
                        failed.set(true);
                    } finally {
                        doneLatch.countDown();
                    }
                });

                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int round = 0; round < 5; round++) {
                            for (int i = 0; i < 20; i++) {
                                tree.put(key((100 + round * 20 + i) & 0xFF), largeValue(100));
                            }
                            tree.flush();
                        }
                    } catch (Exception e) {
                        failed.set(true);
                    } finally {
                        doneLatch.countDown();
                    }
                });

                startLatch.countDown();
                doneLatch.await(30, TimeUnit.SECONDS);
                executor.shutdown();

                assertThat(failed.get()).isFalse();
            }
        }
    }

    @Nested
    class IteratorLifecycle {

        @Test
        void closeReleasesResources() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), value(i));
                }
                tree.flush();

                CloseableIterator<KeyValue> it = tree.scan(null, null);
                assertThat(it.hasNext()).isTrue();
                it.next();
                it.close();
            }
        }

        @Test
        void multipleIteratorsOnSameTree() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                for (int i = 0; i < 20; i++) {
                    tree.put(key(i), value(i));
                }
                tree.flush();

                try (CloseableIterator<KeyValue> it1 = tree.scan(null, null);
                     CloseableIterator<KeyValue> it2 = tree.scan(null, null);
                     CloseableIterator<KeyValue> it3 = tree.scan(null, null)) {

                    List<KeyValue> entries1 = collectEntries(it1);
                    List<KeyValue> entries2 = collectEntries(it2);
                    List<KeyValue> entries3 = collectEntries(it3);

                    assertThat(entries1).hasSize(20);
                    assertThat(entries2).hasSize(20);
                    assertThat(entries3).hasSize(20);
                }
            }
        }

        @Test
        void iteratorSurvivesCompaction() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 30; i++) {
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i));
                    }
                    tree.flush();
                }

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    Thread.sleep(1000);

                    int count = 0;
                    while (it.hasNext()) {
                        KeyValue kv = it.next();
                        assertThat(kv.key()).isNotNull();
                        assertThat(kv.value()).isNotNull();
                        count++;
                    }

                    assertThat(count).isEqualTo(30);
                }
            }
        }

        @Test
        void iteratorSeesConsistentSnapshot() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                for (int i = 0; i < 10; i++) {
                    tree.put(key(i), value(i));
                }
                tree.flush();

                try (CloseableIterator<KeyValue> it = tree.scan(null, null)) {
                    for (int i = 10; i < 20; i++) {
                        tree.put(key(i), value(i));
                    }
                    tree.flush();

                    List<KeyValue> entries = collectEntries(it);

                    assertThat(entries).hasSize(10);
                    for (int i = 0; i < 10; i++) {
                        assertThat(entries.get(i).key()).isEqualTo(key(i));
                    }
                }
            }
        }

        @Test
        void unclosedIteratorPreventsReaderClose() throws Exception {
            LSMConfig config = smallMemtableConfig(1024);

            try (LSMTree tree = LSMTree.open(tempDir, config)) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("initial-" + i));
                }
                tree.flush();

                CloseableIterator<KeyValue> it = tree.scan(null, null);

                for (int batch = 0; batch < 5; batch++) {
                    for (int i = 0; i < 30; i++) {
                        tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i));
                    }
                    tree.flush();
                }
                Thread.sleep(1000);

                int count = 0;
                while (it.hasNext()) {
                    KeyValue kv = it.next();
                    String valueStr = new String(kv.value().toByteArray());
                    assertThat(valueStr).startsWith("initial-");
                    count++;
                }
                assertThat(count).isEqualTo(30);

                it.close();
            }
        }

        @Test
        void closeAfterPartialIteration() {
            try (LSMTree tree = LSMTree.open(tempDir, LSMConfig.create())) {
                for (int i = 0; i < 100; i++) {
                    tree.put(key(i), value(i));
                }
                tree.flush();

                CloseableIterator<KeyValue> it = tree.scan(null, null);

                for (int i = 0; i < 10; i++) {
                    assertThat(it.hasNext()).isTrue();
                    it.next();
                }

                it.close();
            }
        }
    }
}
