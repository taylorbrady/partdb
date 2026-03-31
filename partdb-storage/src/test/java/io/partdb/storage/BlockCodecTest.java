package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BlockCodecTest {

    @Test
    void noneRoundTripsWithoutCopying() {
        byte[] input = "partdb".getBytes(StandardCharsets.UTF_8);

        byte[] compressed = BlockCodec.NONE.compress(input);
        byte[] decompressed = BlockCodec.NONE.decompress(compressed, input.length);

        assertSame(input, compressed);
        assertSame(input, decompressed);
    }

    @Test
    void deflateRoundTrips() {
        byte[] input = repeatedPayload();

        byte[] compressed = BlockCodec.DEFLATE.compress(input);
        byte[] decompressed = BlockCodec.DEFLATE.decompress(compressed, input.length);

        assertFalse(compressed.length == 0);
        assertArrayEquals(input, decompressed);
    }

    @Test
    void deflateRejectsWrongUncompressedSize() {
        byte[] input = repeatedPayload();
        byte[] compressed = BlockCodec.DEFLATE.compress(input);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> BlockCodec.DEFLATE.decompress(compressed, input.length - 1)
        );
        assertTrue(error.getMessage().contains("size mismatch"));
    }

    @Test
    void fromIdRejectsUnknownCodec() {
        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> BlockCodec.fromId((byte) 99)
        );
        assertTrue(error.getMessage().contains("Unknown codec id"));
    }

    private static byte[] repeatedPayload() {
        byte[] chunk = "partdb-storage-block-codec-".getBytes(StandardCharsets.UTF_8);
        byte[] payload = new byte[chunk.length * 64];
        for (int i = 0; i < 64; i++) {
            System.arraycopy(chunk, 0, payload, i * chunk.length, chunk.length);
        }
        return Arrays.copyOf(payload, payload.length);
    }
}
