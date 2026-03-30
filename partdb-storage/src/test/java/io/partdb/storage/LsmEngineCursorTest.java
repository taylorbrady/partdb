package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LsmEngineCursorTest extends LsmEngineTestSupport {

    @Test
    void closeReleasesResources() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 10; i++) {
                tree.put(key(i), value(i), nextRevision());
            }
            tree.flush();

            try (StorageEntryCursor cursor = tree.scan(null, null)) {
                assertTrue(cursor.hasNext());
                assertNotNull(cursor.next());
            }
        }
    }

    @Test
    void multipleCursorsOnSameTree() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 20; i++) {
                tree.put(key(i), value(i), nextRevision());
            }
            tree.flush();

            try (StorageEntryCursor cursor1 = tree.scan(null, null);
                 StorageEntryCursor cursor2 = tree.scan(null, null);
                 StorageEntryCursor cursor3 = tree.scan(null, null)) {

                List<StorageEntry> entries1 = readAll(cursor1);
                List<StorageEntry> entries2 = readAll(cursor2);
                List<StorageEntry> entries3 = readAll(cursor3);

                assertEquals(20, entries1.size());
                assertEquals(20, entries2.size());
                assertEquals(20, entries3.size());
            }
        }
    }

    @Test
    void streamSurvivesCompaction() throws Exception {
        LsmConfig config = smallMemtableConfig(1024);

        try (LsmEngine tree = LsmEngine.open(tempDir, config)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 30; i++) {
                    tree.put(key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                }
                tree.flush();
            }

            try (StorageEntryCursor cursor = tree.scan(null, null)) {
                Thread.sleep(1000);

                List<StorageEntry> entries = readAll(cursor);
                assertEquals(30, entries.size());
                for (StorageEntry e : entries) {
                    assertNotNull(e.key());
                    assertNotNull(e.value());
                }
            }
        }
    }

    @Test
    void closeAfterPartialConsumption() {
        try (LsmEngine tree = LsmEngine.open(tempDir, LsmConfig.defaults())) {
            for (int i = 0; i < 100; i++) {
                tree.put(key(i), value(i), nextRevision());
            }
            tree.flush();

            try (StorageEntryCursor cursor = tree.scan(null, null)) {
                List<StorageEntry> first10 = new ArrayList<>();
                while (cursor.hasNext() && first10.size() < 10) {
                    first10.add(cursor.next());
                }
                assertEquals(10, first10.size());
            }
        }
    }
}
