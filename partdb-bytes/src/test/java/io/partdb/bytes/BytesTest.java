package io.partdb.bytes;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class BytesTest {

    @Test
    void copyOfProducesImmutableValueSemantics() {
        byte[] source = "hello".getBytes(StandardCharsets.UTF_8);

        Bytes bytes = Bytes.copyOf(source);
        source[0] = 'H';

        assertEquals(Bytes.utf8("hello"), bytes);
        assertArrayEquals("hello".getBytes(StandardCharsets.UTF_8), bytes.toByteArray());
        assertNotSame(source, bytes.toByteArray());
    }

    @Test
    void equalsHashCodeAndCompareUseContent() {
        Bytes a = Bytes.fromHex("0102ff");
        Bytes b = Bytes.copyOf(new byte[] {0x01, 0x02, (byte) 0xff});
        Bytes c = Bytes.fromHex("010300");

        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
        assertEquals(0, a.compareTo(b));
        assertEquals(-1, Integer.signum(a.compareTo(c)));
    }

    @Test
    void hexAndUtf8HelpersRoundTrip() {
        Bytes utf8 = Bytes.utf8("partdb");
        Bytes hex = Bytes.fromHex(HexFormat.of().formatHex("partdb".getBytes(StandardCharsets.UTF_8)));

        assertEquals("partdb", utf8.utf8());
        assertEquals(utf8, hex);
    }
}
