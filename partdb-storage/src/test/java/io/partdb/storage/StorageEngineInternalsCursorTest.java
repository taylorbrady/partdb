package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsCursorTest extends StorageEngineInternalTestSupport {

    @Test
    void closeReleasesResources() {
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 10; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            tree.flush();

            try (CloseableIterator<StoredEntry.Value> cursor = tree.scan(ScanBounds.all())) {
                assertTrue(cursor.hasNext());
                assertNotNull(cursor.next());
            }
        }
    }

    @Test
    void multipleCursorsOnSameTree() {
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 20; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            tree.flush();

            try (CloseableIterator<StoredEntry.Value> cursor1 = tree.scan(ScanBounds.all());
                 CloseableIterator<StoredEntry.Value> cursor2 = tree.scan(ScanBounds.all());
                 CloseableIterator<StoredEntry.Value> cursor3 = tree.scan(ScanBounds.all())) {

                List<StoredEntry.Value> entries1 = readAll(cursor1);
                List<StoredEntry.Value> entries2 = readAll(cursor2);
                List<StoredEntry.Value> entries3 = readAll(cursor3);

                assertEquals(20, entries1.size());
                assertEquals(20, entries2.size());
                assertEquals(20, entries3.size());
            }
        }
    }

    @Test
    void streamSurvivesCompaction() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, config)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 30; i++) {
                    put(tree, key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                }
                tree.flush();
            }

            try (CloseableIterator<StoredEntry.Value> cursor = tree.scan(ScanBounds.all())) {
                Thread.sleep(1000);

                List<StoredEntry.Value> entries = readAll(cursor);
                assertEquals(30, entries.size());
                for (StoredEntry.Value e : entries) {
                    assertNotNull(e.key());
                    assertNotNull(e.value());
                }
            }
        }
    }

    @Test
    void closeAfterPartialConsumption() {
        try (StorageEngine tree = StorageEngine.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            tree.flush();

            try (CloseableIterator<StoredEntry.Value> cursor = tree.scan(ScanBounds.all())) {
                List<StoredEntry.Value> first10 = new ArrayList<>();
                while (cursor.hasNext() && first10.size() < 10) {
                    first10.add(cursor.next());
                }
                assertEquals(10, first10.size());
            }
        }
    }
}
