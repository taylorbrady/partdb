package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterTest {

    @Test
    void roundTripRetainsInsertedKeysAcrossVaryingLengths() {
        List<Slice> keys = List.of(
            Slice.empty(),
            Slice.copyOf(new byte[]{0x01}),
            Slice.copyOf(new byte[]{0x01, 0x02}),
            Slice.copyOf(new byte[]{0x01, 0x02, 0x03}),
            Slice.copyOf(new byte[]{0x01, 0x02, 0x03, 0x04}),
            Slice.copyOf(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05}),
            Slice.utf8("alpha"),
            Slice.utf8("longer-storage-key")
        );

        BloomFilter filter = BloomFilter.build(keys, 0.01);
        BloomFilter roundTripped = BloomFilter.from(MemorySegment.ofArray(filter.serialize()));

        for (Slice key : keys) {
            assertTrue(filter.mightContain(key), "expected original filter to contain inserted key");
            assertTrue(roundTripped.mightContain(key), "expected round-tripped filter to contain inserted key");
        }
    }

    @Test
    void emptyBuildRoundTrips() {
        BloomFilter filter = BloomFilter.build(List.of(), 0.01);
        byte[] serialized = filter.serialize();

        assertDoesNotThrow(() -> BloomFilter.from(MemorySegment.ofArray(serialized)));
    }

    @Test
    void rejectsZeroHashFunctions() {
        byte[] serialized = BloomFilter.build(List.of(Slice.utf8("key")), 0.01).serialize();
        ByteBuffer.wrap(serialized).order(ByteOrder.nativeOrder()).putInt(0, 0);

        assertThrows(StorageException.Corruption.class, () -> BloomFilter.from(MemorySegment.ofArray(serialized)));
    }

    @Test
    void rejectsZeroBlocks() {
        byte[] serialized = BloomFilter.build(List.of(Slice.utf8("key")), 0.01).serialize();
        ByteBuffer.wrap(serialized).order(ByteOrder.nativeOrder()).putInt(4, 0);

        assertThrows(StorageException.Corruption.class, () -> BloomFilter.from(MemorySegment.ofArray(serialized)));
    }

    @Test
    void rejectsMismatchedSerializedSize() {
        byte[] serialized = BloomFilter.build(List.of(Slice.utf8("key")), 0.01).serialize();
        byte[] truncated = new byte[serialized.length - 8];
        System.arraycopy(serialized, 0, truncated, 0, truncated.length);

        assertThrows(StorageException.Corruption.class, () -> BloomFilter.from(MemorySegment.ofArray(truncated)));
    }
}
