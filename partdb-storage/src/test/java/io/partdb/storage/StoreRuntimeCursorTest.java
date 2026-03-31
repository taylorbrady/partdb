package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StoreRuntimeCursorTest extends StoreRuntimeTestSupport {

    @Test
    void closeReleasesResources() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 10; i++) {
                tree.put(key(i), value(i), nextRevision());
            }
            tree.flush();

            try (EngineEntryCursor cursor = tree.scan(ScanBounds.all())) {
                assertTrue(cursor.hasNext());
                assertNotNull(cursor.next());
            }
        }
    }

    @Test
    void multipleCursorsOnSameTree() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 20; i++) {
                tree.put(key(i), value(i), nextRevision());
            }
            tree.flush();

            try (EngineEntryCursor cursor1 = tree.scan(ScanBounds.all());
                 EngineEntryCursor cursor2 = tree.scan(ScanBounds.all());
                 EngineEntryCursor cursor3 = tree.scan(ScanBounds.all())) {

                List<EngineEntry> entries1 = readAll(cursor1);
                List<EngineEntry> entries2 = readAll(cursor2);
                List<EngineEntry> entries3 = readAll(cursor3);

                assertEquals(20, entries1.size());
                assertEquals(20, entries2.size());
                assertEquals(20, entries3.size());
            }
        }
    }

    @Test
    void streamSurvivesCompaction() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (StoreRuntime tree = StoreRuntime.open(tempDir, config)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                }
                tree.flush();
            }

            try (EngineEntryCursor cursor = tree.scan(ScanBounds.all())) {
                Thread.sleep(1000);

                List<EngineEntry> entries = readAll(cursor);
                assertEquals(30, entries.size());
                for (EngineEntry e : entries) {
                    assertNotNull(e.key());
                    assertNotNull(e.value());
                }
            }
        }
    }

    @Test
    void closeAfterPartialConsumption() {
        try (StoreRuntime tree = StoreRuntime.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 100; i++) {
                tree.put(key(i), value(i), nextRevision());
            }
            tree.flush();

            try (EngineEntryCursor cursor = tree.scan(ScanBounds.all())) {
                List<EngineEntry> first10 = new ArrayList<>();
                while (cursor.hasNext() && first10.size() < 10) {
                    first10.add(cursor.next());
                }
                assertEquals(10, first10.size());
            }
        }
    }
}
