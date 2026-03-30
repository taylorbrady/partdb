package io.partdb.node;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class KeyValueEntryTest {

    @Test
    void entryDefensivelyCopiesArrays() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var entry = new KeyValueEntry(key, value, 7, 11);

        key[0] = 'K';
        value[0] = 'V';

        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), entry.key());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), entry.value());
        assertNotSame(key, entry.key());
        assertNotSame(value, entry.value());

        byte[] returnedKey = entry.key();
        byte[] returnedValue = entry.value();
        returnedKey[0] = 'X';
        returnedValue[0] = 'Y';

        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), entry.key());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), entry.value());
    }
}
