package io.partdb.node;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KeyValueEntryTest {

    @Test
    void entryHasValueSemantics() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var entry = new KeyValueEntry(Bytes.copyOf(key), Bytes.copyOf(value), 7, 11);

        key[0] = 'K';
        value[0] = 'V';

        assertEquals(Bytes.utf8("key"), entry.key());
        assertEquals(Bytes.utf8("value"), entry.value());
        assertEquals(new KeyValueEntry(Bytes.utf8("key"), Bytes.utf8("value"), 7, 11), entry);
    }
}
