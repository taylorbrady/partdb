package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CompressedBlockTest {

    @Test
    void serializeAndDeserializeRoundTrip() {
        CompressedBlock block = new CompressedBlock(BlockCodec.DEFLATE.id(), 4_096, new byte[]{1, 2, 3, 4, 5});

        CompressedBlock decoded = CompressedBlock.deserialize(MemorySegment.ofArray(block.serialize()));

        assertEquals(block.codecId(), decoded.codecId());
        assertEquals(block.uncompressedSize(), decoded.uncompressedSize());
        assertArrayEquals(block.data(), decoded.data());
    }

    @Test
    void rejectsTooSmallInput() {
        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> CompressedBlock.deserialize(MemorySegment.ofArray(new byte[CompressedBlock.HEADER_SIZE]))
        );

        assertTrue(error.getMessage().contains("too small"));
    }

    @Test
    void rejectsChecksumMismatch() {
        CompressedBlock block = new CompressedBlock(BlockCodec.NONE.id(), 64, new byte[]{9, 8, 7, 6});
        byte[] encoded = block.serialize();
        encoded[CompressedBlock.HEADER_SIZE] ^= 0x01;

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> CompressedBlock.deserialize(MemorySegment.ofArray(encoded))
        );

        assertTrue(error.getMessage().contains("checksum mismatch"));
    }

    @Test
    void rejectsNegativeUncompressedSize() {
        CompressedBlock block = new CompressedBlock(BlockCodec.NONE.id(), 64, new byte[]{1, 2, 3});
        byte[] encoded = block.serialize();
        writeInt(encoded, 1, -1);
        rewriteChecksum(encoded);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> CompressedBlock.deserialize(MemorySegment.ofArray(encoded))
        );

        assertTrue(error.getMessage().contains("Negative compressed block uncompressed size"));
    }

    @Test
    void rejectsDeclaredCompressedSizeMismatch() {
        CompressedBlock block = new CompressedBlock(BlockCodec.DEFLATE.id(), 1_024, new byte[]{1, 2, 3, 4, 5});
        byte[] encoded = block.serialize();
        writeInt(encoded, 5, block.data().length - 1);
        rewriteChecksum(encoded);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> CompressedBlock.deserialize(MemorySegment.ofArray(encoded))
        );

        assertTrue(error.getMessage().contains("size mismatch"));
    }

    private static void rewriteChecksum(byte[] encoded) {
        CRC32C crc = new CRC32C();
        crc.update(encoded, 0, encoded.length - Integer.BYTES);
        writeInt(encoded, encoded.length - Integer.BYTES, (int) crc.getValue());
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES)
            .order(ByteOrder.nativeOrder())
            .putInt(value);
    }
}
