package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BloomFilterTest {

    @Test
    void hasNoFalseNegativesAndBoundedFalsePositivesAfterRoundTrip() {
        List<Slice> keys = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            keys.add(Slice.utf8("present-" + i));
        }
        BloomFilter filter = BloomFilter.decode(MemorySegment.ofArray(BloomFilter.build(keys, 0.01).encode()));

        keys.forEach(key -> assertTrue(filter.mightContain(key)));
        int falsePositives = 0;
        for (int i = 0; i < 10_000; i++) {
            if (filter.mightContain(Slice.utf8("absent-" + i))) {
                falsePositives++;
            }
        }
        assertTrue(falsePositives < 300, "false positives: " + falsePositives);
    }

    @Test
    void emptyFilterRejectsEverything() {
        BloomFilter filter = BloomFilter.build(List.of(), 0.01);
        assertTrue(!filter.mightContain(Slice.utf8("anything")));
    }

    @Test
    void malformedFiltersAreCorruption() {
        byte[] valid = BloomFilter.build(List.of(Slice.utf8("a")), 0.01).encode();
        assertThrows(StoreException.Corruption.class, () -> BloomFilter.decode(MemorySegment.ofArray(new byte[4])));
        byte[] truncated = Arrays.copyOf(valid, valid.length - 8);
        assertThrows(StoreException.Corruption.class, () -> BloomFilter.decode(MemorySegment.ofArray(truncated)));
    }
}
