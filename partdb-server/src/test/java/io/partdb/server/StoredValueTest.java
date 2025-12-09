package io.partdb.server;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StoredValueTest {

    @Test
    void encodeDecodeRoundTrip() {
        var original = new StoredValue(
            "hello".getBytes(),
            42L,
            123L
        );

        var encoded = original.encode();
        var decoded = StoredValue.decode(encoded);

        assertArrayEquals(original.value(), decoded.value());
        assertEquals(original.version(), decoded.version());
        assertEquals(original.leaseId(), decoded.leaseId());
    }

    @Test
    void preservesVersion() {
        var stored = new StoredValue("data".getBytes(), 999L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(999L, decoded.version());
    }

    @Test
    void preservesLeaseId() {
        var stored = new StoredValue("data".getBytes(), 1L, 456L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(456L, decoded.leaseId());
    }

    @Test
    void handlesZeroLeaseId() {
        var stored = new StoredValue("data".getBytes(), 1L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(0L, decoded.leaseId());
    }

    @Test
    void handlesEmptyValue() {
        var stored = new StoredValue(new byte[0], 1L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(0, decoded.value().length);
    }

    @Test
    void handlesLargeValue() {
        byte[] largeData = new byte[10_000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        var stored = new StoredValue(largeData, 1L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertArrayEquals(largeData, decoded.value());
    }

    @Test
    void handlesMaxLongValues() {
        var stored = new StoredValue(
            "data".getBytes(),
            Long.MAX_VALUE,
            Long.MAX_VALUE
        );

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(Long.MAX_VALUE, decoded.version());
        assertEquals(Long.MAX_VALUE, decoded.leaseId());
    }

    @Test
    void encodedSizeIsHeaderPlusValue() {
        byte[] value = "test".getBytes();
        var stored = new StoredValue(value, 1L, 1L);

        var encoded = stored.encode();

        assertEquals(16 + value.length, encoded.length);
    }

    @Test
    void differentValuesProduceDifferentEncodings() {
        var stored1 = new StoredValue("aaa".getBytes(), 1L, 1L);
        var stored2 = new StoredValue("bbb".getBytes(), 1L, 1L);

        assertFalse(java.util.Arrays.equals(stored1.encode(), stored2.encode()));
    }

    @Test
    void differentVersionsProduceDifferentEncodings() {
        var stored1 = new StoredValue("data".getBytes(), 1L, 0L);
        var stored2 = new StoredValue("data".getBytes(), 2L, 0L);

        assertFalse(java.util.Arrays.equals(stored1.encode(), stored2.encode()));
    }
}
