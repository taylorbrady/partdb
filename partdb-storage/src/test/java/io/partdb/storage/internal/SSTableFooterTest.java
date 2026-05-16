package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSTableFooterTest {

    @Test
    void serializeAndDeserializeRoundTrip() {
        SSTableFooter footer = footer();

        SSTableFooter decoded = SSTableFooter.deserialize(MemorySegment.ofArray(footer.serialize()));

        assertEquals(footer.bloomFilterOffset(), decoded.bloomFilterOffset());
        assertEquals(footer.bloomFilterSize(), decoded.bloomFilterSize());
        assertEquals(footer.indexOffset(), decoded.indexOffset());
        assertEquals(footer.blockCount(), decoded.blockCount());
        assertEquals(footer.smallestKey(), decoded.smallestKey());
        assertEquals(footer.largestKey(), decoded.largestKey());
        assertEquals(footer.smallestRevision(), decoded.smallestRevision());
        assertEquals(footer.largestRevision(), decoded.largestRevision());
        assertEquals(footer.entryCount(), decoded.entryCount());
    }

    @Test
    void rejectsTooSmallFooter() {
        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableFooter.deserialize(MemorySegment.ofArray(new byte[3]))
        );

        assertTrue(error.getMessage().contains("too small"));
    }

    @Test
    void rejectsInvalidFooterSizeField() {
        byte[] encoded = footer().serialize();
        writeInt(encoded, encoded.length - (Integer.BYTES * 2), encoded.length - 1);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableFooter.deserialize(MemorySegment.ofArray(encoded))
        );

        assertTrue(error.getMessage().contains("Invalid footer size"));
    }

    @Test
    void rejectsInvalidSmallestKeyLength() {
        byte[] encoded = footer().serialize();
        writeInt(encoded, 24, Integer.MAX_VALUE);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableFooter.deserialize(MemorySegment.ofArray(encoded))
        );

        assertTrue(error.getMessage().contains("smallest key length"));
    }

    @Test
    void rejectsChecksumMismatch() {
        byte[] encoded = footer().serialize();
        encoded[0] ^= 0x01;

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableFooter.deserialize(MemorySegment.ofArray(encoded))
        );

        assertTrue(error.getMessage().contains("checksum mismatch"));
    }

    private static SSTableFooter footer() {
        return new SSTableFooter(
            128L,
            64,
            1_024L,
            3,
            Slice.utf8("alpha"),
            Slice.utf8("omega"),
            10,
            20,
            42,
            0
        );
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES)
            .order(ByteOrder.nativeOrder())
            .putInt(value);
    }
}
