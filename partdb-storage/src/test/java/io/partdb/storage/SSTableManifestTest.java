package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.zip.CRC32C;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SSTableManifestTest {

    @TempDir
    Path tempDir;

    @Test
    void writeToAndReadFromRoundTrip() {
        SSTableManifest manifest = manifest();

        manifest.writeTo(tempDir);
        SSTableManifest decoded = SSTableManifest.readFrom(tempDir);

        assertEquals(manifest, decoded);
    }

    @Test
    void readFromMissingManifestReturnsEmptyManifest() {
        assertEquals(new SSTableManifest(0, 0, List.of()), SSTableManifest.readFrom(tempDir));
    }

    @Test
    void rejectsInvalidMagic() {
        byte[] encoded = manifest().toBytes();
        writeInt(encoded, 0, 0x12345678);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableManifest.fromBytes(encoded)
        );

        assertTrue(error.getMessage().contains("magic number"));
    }

    @Test
    void rejectsChecksumMismatch() {
        byte[] encoded = manifest().toBytes();
        encoded[encoded.length - Integer.BYTES - 1] ^= 0x01;

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableManifest.fromBytes(encoded)
        );

        assertTrue(error.getMessage().contains("checksum mismatch"));
    }

    @Test
    void rejectsTrailingManifestData() {
        byte[] encoded = manifest().toBytes();
        writeInt(encoded, 24, 0);
        rewriteChecksum(encoded);

        StorageException.Corruption error = assertThrows(
            StorageException.Corruption.class,
            () -> SSTableManifest.fromBytes(encoded)
        );

        assertTrue(error.getMessage().contains("Trailing SSTable manifest data"));
    }

    private static SSTableManifest manifest() {
        return new SSTableManifest(
            7,
            11,
            List.of(
                new SSTableMetadata(1, 0, Slice.utf8("alpha"), Slice.utf8("delta"), 1, 4, 512, 12),
                new SSTableMetadata(2, 1, Slice.utf8("echo"), Slice.utf8("omega"), 5, 9, 768, 16)
            )
        );
    }

    private static void rewriteChecksum(byte[] encoded) {
        CRC32C crc = new CRC32C();
        crc.update(encoded, 0, encoded.length - Integer.BYTES);
        writeInt(encoded, encoded.length - Integer.BYTES, (int) crc.getValue());
    }

    private static void writeInt(byte[] bytes, int offset, int value) {
        ByteBuffer.wrap(bytes, offset, Integer.BYTES)
            .putInt(value);
    }
}
