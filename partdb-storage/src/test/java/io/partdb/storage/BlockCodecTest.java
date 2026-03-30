package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BlockCodecTest {

    @Test
    void noneRoundTripsWithoutCopying() {
        byte[] input = "partdb".getBytes(StandardCharsets.UTF_8);

        byte[] compressed = BlockCodec.NONE.compress(input);
        byte[] decompressed = BlockCodec.NONE.decompress(compressed, input.length);

        assertThat(compressed).isSameAs(input);
        assertThat(decompressed).isSameAs(input);
    }

    @Test
    void deflateRoundTrips() {
        byte[] input = repeatedPayload();

        byte[] compressed = BlockCodec.DEFLATE.compress(input);
        byte[] decompressed = BlockCodec.DEFLATE.decompress(compressed, input.length);

        assertThat(compressed).isNotEmpty();
        assertThat(decompressed).isEqualTo(input);
    }

    @Test
    void deflateRejectsWrongUncompressedSize() {
        byte[] input = repeatedPayload();
        byte[] compressed = BlockCodec.DEFLATE.compress(input);

        assertThatThrownBy(() -> BlockCodec.DEFLATE.decompress(compressed, input.length - 1))
            .isInstanceOf(StorageException.Corruption.class)
            .hasMessageContaining("size mismatch");
    }

    @Test
    void fromIdRejectsUnknownCodec() {
        assertThatThrownBy(() -> BlockCodec.fromId((byte) 99))
            .isInstanceOf(StorageException.Corruption.class)
            .hasMessageContaining("Unknown codec id");
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
