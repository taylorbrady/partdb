package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BlockTest {

    @Test
    void roundTripsEntriesAcrossRestartIntervals() {
        for (int restartInterval : new int[]{1, 3, 16}) {
            roundTrip(restartInterval);
        }
    }

    private static void roundTrip(int restartInterval) {
        List<InternalEntry> entries = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Slice key = Slice.utf8("tenant/key-%04d".formatted(i));
            entries.add(new InternalEntry.Value(new InternalKey(key, 20), Slice.utf8("v" + i)));
            entries.add(i % 4 == 0 ? new InternalEntry.Tombstone(new InternalKey(key, 10)) : new InternalEntry.Value(new InternalKey(key, 10), Slice.utf8("old" + i)));
        }
        Block block = block(restartInterval, entries);

        List<InternalEntry> decoded = new ArrayList<>();
        block.iterator().forEachRemaining(decoded::add);
        assertEquals(entries, decoded);

        Slice key = Slice.utf8("tenant/key-0040");
        assertEquals(Optional.of(entries.get(80)), block.get(key, 25));
        assertEquals(Optional.of(entries.get(81)), block.get(key, 15));
        assertEquals(Optional.empty(), block.get(key, 5));
        assertEquals(Optional.empty(), block.get(Slice.utf8("tenant/key-0040a"), 25));
        assertEquals(Optional.empty(), block.get(Slice.utf8("a"), 25));
        assertEquals(Optional.empty(), block.get(Slice.utf8("z"), 25));
    }

    @Test
    void seekPositionsAtFirstKeyAtOrAfterTarget() {
        List<InternalEntry> entries = new ArrayList<>();
        for (int i = 0; i < 50; i += 2) {
            entries.add(new InternalEntry.Value(new InternalKey(Slice.utf8("k%02d".formatted(i)), 1), Slice.utf8("v")));
        }
        Block block = block(4, entries);

        Iterator<InternalEntry> cursor = block.seek(Slice.utf8("k11"));
        assertEquals(Slice.utf8("k12"), cursor.next().userKey());
        assertEquals(Slice.utf8("k00"), block.seek(Slice.EMPTY).next().userKey());
        assertEquals(false, block.seek(Slice.utf8("k99")).hasNext());
    }

    @Test
    void writerRejectsOutOfOrderEntries() {
        BlockWriter writer = new BlockWriter(4);
        writer.add(new InternalEntry.Tombstone(new InternalKey(Slice.utf8("b"), 1)));
        assertThrows(IllegalArgumentException.class, () -> writer.add(new InternalEntry.Tombstone(new InternalKey(Slice.utf8("a"), 1))));
        assertThrows(IllegalArgumentException.class, () -> writer.add(new InternalEntry.Tombstone(new InternalKey(Slice.utf8("b"), 2))));
    }

    @Test
    void malformedBlocksAreCorruption() {
        BlockWriter writer = new BlockWriter(2);
        writer.add(new InternalEntry.Value(new InternalKey(Slice.utf8("a"), 1), Slice.utf8("1")));
        byte[] valid = writer.finish();

        assertThrows(StoreException.Corruption.class, () -> Block.from(MemorySegment.ofArray(new byte[3])));
        byte[] badCount = valid.clone();
        badCount[badCount.length - 4] = 0;
        assertThrows(StoreException.Corruption.class, () -> Block.from(MemorySegment.ofArray(badCount)));
        byte[] truncatedEntry = valid.clone();
        truncatedEntry[2] = 100;
        assertThrows(StoreException.Corruption.class, () -> Block.from(MemorySegment.ofArray(truncatedEntry)).iterator().next());
    }

    @Test
    void codecFramesRoundTripAndDetectCorruption() {
        byte[] raw = "partdb-block-codec-".repeat(200).getBytes();
        for (StoreOptions.Compression compression : StoreOptions.Compression.values()) {
            byte[] frame = BlockCodec.encode(compression, raw);
            assertArrayEquals(raw, BlockCodec.decode(MemorySegment.ofArray(frame)));
            byte[] corrupt = frame.clone();
            corrupt[frame.length / 2] ^= 1;
            assertThrows(StoreException.Corruption.class, () -> BlockCodec.decode(MemorySegment.ofArray(corrupt)));
        }
        byte[] deflated = BlockCodec.encode(StoreOptions.Compression.DEFLATE, raw);
        assertEquals(true, deflated.length < raw.length / 4);
    }

    private static Block block(int restartInterval, List<InternalEntry> entries) {
        BlockWriter writer = new BlockWriter(restartInterval);
        entries.forEach(writer::add);
        return Block.from(MemorySegment.ofArray(writer.finish()));
    }
}
