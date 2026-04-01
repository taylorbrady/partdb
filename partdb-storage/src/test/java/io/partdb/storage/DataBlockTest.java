package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataBlockTest {

    @Test
    void findsAndIteratesEntriesAcrossRestartRegions() {
        DataBlockWriter writer = new DataBlockWriter(2);
        writer.append(value("alpha", "v1", 1));
        writer.append(value("alphabet", "v2", 2));
        writer.append(value("alphanumeric", "v3", 3));
        writer.append(tombstone("beta", 4));

        DataBlockReader reader = DataBlockReader.from(MemorySegment.ofArray(writer.finish()));

        assertEquals("alpha", utf8(reader.firstKey()));
        assertEquals("beta", utf8(reader.lastKey()));
        assertEquals(4, reader.entryCount());
        StoredEntry.Value alphabet = assertInstanceOf(
            StoredEntry.Value.class,
            reader.find(Slice.utf8("alphabet")).orElseThrow()
        );
        StoredEntry.Value alphanumeric = assertInstanceOf(
            StoredEntry.Value.class,
            reader.find(Slice.utf8("alphanumeric")).orElseThrow()
        );
        assertEquals("v2", utf8(alphabet.value()));
        assertEquals("v3", utf8(alphanumeric.value()));

        List<String> keys = new ArrayList<>();
        DataBlockCursor cursor = reader.cursor();
        while (cursor.hasNext()) {
            keys.add(utf8(cursor.next().key()));
        }
        assertEquals(List.of("alpha", "alphabet", "alphanumeric", "beta"), keys);
    }

    @Test
    void cursorAtOrAfterStartsInsideRestartRegion() {
        DataBlockWriter writer = new DataBlockWriter(2);
        writer.append(value("alpha", "v1", 1));
        writer.append(value("alphabet", "v2", 2));
        writer.append(value("alphanumeric", "v3", 3));
        writer.append(value("beta", "v4", 4));

        DataBlockReader reader = DataBlockReader.from(MemorySegment.ofArray(writer.finish()));
        DataBlockCursor cursor = reader.cursorAtOrAfter(Slice.utf8("alphabetic"));

        List<String> keys = new ArrayList<>();
        while (cursor.hasNext()) {
            keys.add(utf8(cursor.next().key()));
        }

        assertEquals(List.of("alphanumeric", "beta"), keys);
    }

    @Test
    void rejectsInvalidFirstRestartOffset() {
        DataBlockWriter writer = new DataBlockWriter(2);
        writer.append(value("alpha", "v1", 1));
        writer.append(value("alphabet", "v2", 2));

        byte[] encoded = writer.finish();
        ByteBuffer.wrap(encoded)
            .order(ByteOrder.nativeOrder())
            .putInt(encoded.length - (Integer.BYTES * 3), 1);

        assertThrows(StorageException.Corruption.class, () -> DataBlockReader.from(MemorySegment.ofArray(encoded)));
    }

    @Test
    void rejectsRestartEntryWithSharedPrefix() {
        DataBlockWriter writer = new DataBlockWriter(1);
        writer.append(value("alpha", "v1", 1));
        writer.append(value("beta", "v2", 2));

        byte[] encoded = writer.finish();
        encoded[0] = 0x01;

        assertThrows(StorageException.Corruption.class, () -> DataBlockReader.from(MemorySegment.ofArray(encoded)));
    }

    private static StoredEntry.Value value(String key, String value, long revision) {
        return new StoredEntry.Value(Slice.utf8(key), Slice.utf8(value), revision);
    }

    private static StoredEntry.Tombstone tombstone(String key, long revision) {
        return new StoredEntry.Tombstone(Slice.utf8(key), revision);
    }

    private static String utf8(Slice value) {
        return new String(value.toByteArray(), StandardCharsets.UTF_8);
    }
}
