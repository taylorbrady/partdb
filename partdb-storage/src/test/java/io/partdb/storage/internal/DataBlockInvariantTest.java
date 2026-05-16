package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataBlockInvariantTest {

    @Test
    void randomizedRoundTripPreservesEntriesAndPointLookups() {
        Random random = new Random(24680);

        for (int round = 0; round < 50; round++) {
            String prefix = "tenant-%02d/session-%02d/key-".formatted(round % 7, random.nextInt(4));
            int restartInterval = 1 + random.nextInt(8);
            int entryCount = 1 + random.nextInt(40);

            DataBlockWriter writer = new DataBlockWriter(restartInterval);
            List<StoredEntry> expected = new ArrayList<>(entryCount);

            for (int i = 0; i < entryCount; i++) {
                Slice key = Slice.utf8(prefix + "%03d".formatted(i));
                StoredEntry entry;
                if (random.nextInt(5) == 0) {
                    entry = new StoredEntry.Tombstone(key, round * 1_000L + i);
                } else {
                    byte[] value = new byte[random.nextInt(65)];
                    random.nextBytes(value);
                    entry = new StoredEntry.Value(key, Slice.copyOf(value), round * 1_000L + i);
                }
                writer.append(entry);
                expected.add(entry);
            }

            DataBlockReader reader = DataBlockReader.from(MemorySegment.ofArray(writer.finish()));

            assertEquals(expected.getFirst().key(), reader.firstKey());
            assertEquals(expected.getLast().key(), reader.lastKey());
            assertEquals(expected.size(), reader.entryCount());
            assertEquals(expected, drain(reader.cursor()));

            for (StoredEntry entry : expected) {
                assertEquals(Optional.of(entry), reader.find(entry.key()));
            }

            assertTrue(reader.find(Slice.utf8(prefix + "999")).isEmpty());
        }
    }

    private static List<StoredEntry> drain(DataBlockCursor cursor) {
        List<StoredEntry> result = new ArrayList<>();
        while (cursor.hasNext()) {
            result.add(cursor.next().toStoredEntry());
        }
        return result;
    }
}
