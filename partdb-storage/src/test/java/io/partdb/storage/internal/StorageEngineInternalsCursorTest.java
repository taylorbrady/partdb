package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageEngineInternalsCursorTest extends StorageEngineInternalTestSupport {

    @Test
    void closeReleasesResources() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            for (int i = 0; i < 10; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            drainToDurableState(tree);

            try (Scan cursor = tree.scan(KeyRange.all())) {
                var iterator = cursor.iterator();
                assertTrue(iterator.hasNext());
                assertNotNull(iterator.next());
            }
        }
    }

    @Test
    void multipleCursorsOnSameTree() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            for (int i = 0; i < 20; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            drainToDurableState(tree);

            try (Scan cursor1 = tree.scan(KeyRange.all());
                 Scan cursor2 = tree.scan(KeyRange.all());
                 Scan cursor3 = tree.scan(KeyRange.all())) {

                List<EntryRecord> entries1 = readAll(cursor1);
                List<EntryRecord> entries2 = readAll(cursor2);
                List<EntryRecord> entries3 = readAll(cursor3);

                assertEquals(20, entries1.size());
                assertEquals(20, entries2.size());
                assertEquals(20, entries3.size());
            }
        }
    }

    @Test
    void streamSurvivesCompaction() throws Exception {
        StorageOptions options = smallWriteBufferOptions(1024);

        try (StorageEngine tree = StorageEngine.open(tempDir, options)) {
            for (int batch = 0; batch < 5; batch++) {
                for (int i = 0; i < 30; i++) {
                    put(tree, key(String.format("key-%03d", i)), value("v" + batch + "-" + i), nextRevision());
                }
                drainToDurableState(tree);
            }

            try (Scan cursor = tree.scan(KeyRange.all())) {
                Thread.sleep(1000);

                List<EntryRecord> entries = readAll(cursor);
                assertEquals(30, entries.size());
                for (EntryRecord e : entries) {
                    assertNotNull(e.key());
                    assertNotNull(e.value());
                }
            }
        }
    }

    @Test
    void closeAfterPartialConsumption() {
        try (StorageEngine tree = StorageEngine.open(tempDir, StorageOptions.defaults())) {
            for (int i = 0; i < 100; i++) {
                put(tree, key(i), value(i), nextRevision());
            }
            drainToDurableState(tree);

            try (Scan cursor = tree.scan(KeyRange.all())) {
                List<EntryRecord> first10 = new ArrayList<>();
                var iterator = cursor.iterator();
                while (iterator.hasNext() && first10.size() < 10) {
                    first10.add(iterator.next());
                }
                assertEquals(10, first10.size());
            }
        }
    }
}
