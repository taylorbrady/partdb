package io.partdb.server;

import io.partdb.common.ByteArray;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class StoredValueTest {

    @Test
    void encodeDecodeRoundTrip() {
        var original = new StoredValue(
            ByteArray.wrap("hello".getBytes()),
            42L,
            123L
        );

        var encoded = original.encode();
        var decoded = StoredValue.decode(encoded);

        assertArrayEquals(original.value().toByteArray(), decoded.value().toByteArray());
        assertEquals(original.version(), decoded.version());
        assertEquals(original.leaseId(), decoded.leaseId());
    }

    @Test
    void preservesVersion() {
        var stored = new StoredValue(ByteArray.wrap("data".getBytes()), 999L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(999L, decoded.version());
    }

    @Test
    void preservesLeaseId() {
        var stored = new StoredValue(ByteArray.wrap("data".getBytes()), 1L, 456L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(456L, decoded.leaseId());
    }

    @Test
    void handlesZeroLeaseId() {
        var stored = new StoredValue(ByteArray.wrap("data".getBytes()), 1L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(0L, decoded.leaseId());
    }

    @Test
    void handlesEmptyValue() {
        var stored = new StoredValue(ByteArray.wrap(new byte[0]), 1L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertEquals(0, decoded.value().toByteArray().length);
    }

    @Test
    void handlesLargeValue() {
        byte[] largeData = new byte[10_000];
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        var stored = new StoredValue(ByteArray.wrap(largeData), 1L, 0L);

        var decoded = StoredValue.decode(stored.encode());

        assertArrayEquals(largeData, decoded.value().toByteArray());
    }

    @Test
    void handlesMaxLongValues() {
        var stored = new StoredValue(
            ByteArray.wrap("data".getBytes()),
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
        var stored = new StoredValue(ByteArray.wrap(value), 1L, 1L);

        var encoded = stored.encode();

        assertEquals(16 + value.length, encoded.toByteArray().length);
    }

    @Test
    void differentValuesProduceDifferentEncodings() {
        var stored1 = new StoredValue(ByteArray.wrap("aaa".getBytes()), 1L, 1L);
        var stored2 = new StoredValue(ByteArray.wrap("bbb".getBytes()), 1L, 1L);

        assertFalse(java.util.Arrays.equals(
            stored1.encode().toByteArray(),
            stored2.encode().toByteArray()
        ));
    }

    @Test
    void differentVersionsProduceDifferentEncodings() {
        var stored1 = new StoredValue(ByteArray.wrap("data".getBytes()), 1L, 0L);
        var stored2 = new StoredValue(ByteArray.wrap("data".getBytes()), 2L, 0L);

        assertFalse(java.util.Arrays.equals(
            stored1.encode().toByteArray(),
            stored2.encode().toByteArray()
        ));
    }
}
