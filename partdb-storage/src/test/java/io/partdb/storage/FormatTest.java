package io.partdb.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static io.partdb.storage.Fixtures.put;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormatTest {

    @TempDir
    Path directory;

    @Test
    void everyFileFormatStartsWithALittleEndianMagicAndVersion() throws IOException {
        Path root = directory.resolve("store");
        byte[] checkpoint;
        try (Store store = Store.open(root, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "1")));
            checkpoint = Fixtures.checkpoint(store);
        }
        Path generation = Fixtures.generation(root);
        byte[] table = Files.readAllBytes(generation.resolve(Fixtures.tableFiles(generation).getFirst()));
        byte[] manifest = Files.readAllBytes(generation.resolve(Manifest.FILE_NAME));

        assertHeader("PDBT", table);
        assertHeader("PDBM", manifest);
        assertHeader("PDBC", checkpoint);
        assertEquals(1L, ByteBuffer.wrap(checkpoint, 8, 8).order(ByteOrder.LITTLE_ENDIAN).getLong());
    }

    @Test
    void tableRoundTripsThroughWriterAndReader() {
        Path path = directory.resolve("000001.sst");
        try (TableWriter writer = TableWriter.create(path, 1, 2, StoreOptions.Table.defaults().withBlockSizeBytes(256))) {
            for (int i = 0; i < 500; i++) {
                writer.add(entry(i, i % 10 == 0));
            }
            TableMetadata metadata = writer.finish();
            assertEquals(Slice.utf8(Fixtures.key(0)), metadata.smallest());
            assertEquals(Slice.utf8(Fixtures.key(499)), metadata.largest());
        }
        TableReader reader = TableReader.open(path, 1, 2, new BlockCache(0));
        try {
            assertEquals(2, reader.metadata().level());
            for (int i = 0; i < 500; i++) {
                InternalEntry found = reader.get(Slice.utf8(Fixtures.key(i)), Long.MAX_VALUE).orElseThrow();
                assertEquals(entry(i, i % 10 == 0), found);
            }
            assertEquals(Optional.empty(), reader.get(Slice.utf8("missing"), Long.MAX_VALUE));
            assertEquals(Optional.empty(), reader.get(Slice.utf8(Fixtures.key(5)), 4));

            var scan = reader.scan(new Bounds(Slice.utf8(Fixtures.key(100)), Slice.utf8(Fixtures.key(200))));
            int count = 0;
            while (scan.hasNext()) {
                assertEquals(entry(100 + count, (100 + count) % 10 == 0), scan.next());
                count++;
            }
            assertEquals(100, count);
        } finally {
            reader.release();
        }
    }

    @Test
    void corruptTablesAreRejected() throws IOException {
        Path path = directory.resolve("000001.sst");
        try (TableWriter writer = TableWriter.create(path, 1, 0, StoreOptions.Table.defaults().withBlockSizeBytes(256))) {
            for (int i = 0; i < 200; i++) {
                writer.add(entry(i, false));
            }
            writer.finish();
        }
        byte[] valid = Files.readAllBytes(path);

        assertTableCorrupt(path, withInt(valid, 0, 0x12345678));
        assertTableCorrupt(path, withInt(valid, 4, 99));
        assertTableCorrupt(path, flip(valid, valid.length - 12));
        assertTableCorrupt(path, flip(valid, valid.length - 80));
        assertTableCorrupt(path, Arrays.copyOf(valid, valid.length - 1));

        Files.write(path, flip(valid, 20));
        TableReader reader = TableReader.open(path, 1, 0, new BlockCache(0));
        try {
            assertThrows(StoreException.Corruption.class, () -> reader.get(Slice.utf8(Fixtures.key(0)), Long.MAX_VALUE));
        } finally {
            reader.release();
        }
    }

    @Test
    void corruptManifestsAreRejected() {
        byte[] valid = new Manifest(7, 3, List.of(new Manifest.Placement(1, 0), new Manifest.Placement(2, 1))).encode();
        assertEquals(new Manifest(7, 3, List.of(new Manifest.Placement(1, 0), new Manifest.Placement(2, 1))), Manifest.decode(valid));

        assertThrows(StoreException.Corruption.class, () -> Manifest.decode(withInt(valid, 0, 1)));
        assertThrows(StoreException.Corruption.class, () -> Manifest.decode(withInt(valid, 4, 2)));
        assertThrows(StoreException.Corruption.class, () -> Manifest.decode(flip(valid, 10)));
        assertThrows(StoreException.Corruption.class, () -> Manifest.decode(Arrays.copyOf(valid, 12)));
    }

    @Test
    void storeWithCorruptManifestRefusesToOpen() throws IOException {
        Path root = directory.resolve("store");
        try (Store store = Store.open(root, StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "1")));
        }
        Path manifest = Fixtures.generation(root).resolve(Manifest.FILE_NAME);
        Files.write(manifest, flip(Files.readAllBytes(manifest), 9));

        assertThrows(StoreException.Corruption.class, () -> Store.open(root, StoreOptions.defaults()));
    }

    @Test
    void corruptCheckpointsAreRejected() {
        byte[] valid;
        try (Store store = Store.open(directory.resolve("source"), StoreOptions.defaults())) {
            store.apply(1, List.of(put("a", "1")));
            valid = Fixtures.checkpoint(store);
        }
        try (Store target = Store.open(directory.resolve("target"), StoreOptions.defaults())) {
            assertRestoreCorrupt(target, withInt(valid, 0, 0));
            assertRestoreCorrupt(target, withInt(valid, 4, 2));
            assertRestoreCorrupt(target, flip(valid, 9));
            assertRestoreCorrupt(target, flip(valid, valid.length - 1));
            assertRestoreCorrupt(target, flip(valid, valid.length - 40));
            assertRestoreCorrupt(target, Arrays.copyOf(valid, 10));
            assertEquals(0, target.appliedIndex());
            assertEquals(1, Fixtures.restore(target, valid));
        }
    }

    private static InternalEntry entry(int i, boolean tombstone) {
        InternalKey key = new InternalKey(Slice.utf8(Fixtures.key(i)), 5 + i);
        return tombstone ? new InternalEntry.Tombstone(key) : new InternalEntry.Value(key, Slice.utf8("value-" + i));
    }

    private static void assertHeader(String magic, byte[] file) {
        assertArrayEquals(magic.getBytes(StandardCharsets.US_ASCII), Arrays.copyOf(file, 4));
        assertEquals(1, ByteBuffer.wrap(file, 4, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
    }

    private static void assertTableCorrupt(Path path, byte[] contents) throws IOException {
        Files.write(path, contents);
        assertThrows(StoreException.Corruption.class, () -> TableReader.open(path, 1, 0, new BlockCache(0)));
    }

    private static void assertRestoreCorrupt(Store store, byte[] checkpoint) {
        assertThrows(StoreException.Corruption.class, () -> store.restore(new ByteArrayInputStream(checkpoint)));
    }

    private static byte[] withInt(byte[] bytes, int offset, int value) {
        byte[] copy = bytes.clone();
        ByteBuffer.wrap(copy).order(ByteOrder.LITTLE_ENDIAN).putInt(offset, value);
        return copy;
    }

    private static byte[] flip(byte[] bytes, int offset) {
        byte[] copy = bytes.clone();
        copy[offset] ^= 0x5a;
        return copy;
    }
}
