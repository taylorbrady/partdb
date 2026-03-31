package io.partdb.bytes;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;

public final class Bytes implements Comparable<Bytes> {
    private static final HexFormat HEX = HexFormat.of();

    public static final Bytes EMPTY = new Bytes(new byte[0]);

    private final byte[] data;

    private Bytes(byte[] data) {
        this.data = data;
    }

    public static Bytes copyOf(byte[] data) {
        Objects.requireNonNull(data, "data must not be null");
        if (data.length == 0) {
            return EMPTY;
        }
        return new Bytes(data.clone());
    }

    public static Bytes copyOf(byte[] data, int offset, int length) {
        Objects.requireNonNull(data, "data must not be null");
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException("Invalid offset/length for byte array of length " + data.length);
        }
        if (length == 0) {
            return EMPTY;
        }
        return new Bytes(Arrays.copyOfRange(data, offset, offset + length));
    }

    public static Bytes utf8(String value) {
        Objects.requireNonNull(value, "value must not be null");
        return copyOf(value.getBytes(StandardCharsets.UTF_8));
    }

    public static Bytes fromHex(CharSequence hex) {
        Objects.requireNonNull(hex, "hex must not be null");
        return copyOf(HEX.parseHex(hex));
    }

    public int size() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    public byte byteAt(int index) {
        return data[index];
    }

    public byte[] toByteArray() {
        return data.clone();
    }

    public ByteBuffer asReadOnlyByteBuffer() {
        return ByteBuffer.wrap(data).asReadOnlyBuffer();
    }

    public String toHex() {
        return HEX.formatHex(data);
    }

    public String utf8() {
        return new String(data, StandardCharsets.UTF_8);
    }

    @Override
    public int compareTo(Bytes other) {
        Objects.requireNonNull(other, "other must not be null");
        return Arrays.compareUnsigned(data, other.data);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Bytes other && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        int previewSize = Math.min(data.length, 16);
        String preview = HEX.formatHex(data, 0, previewSize);
        return "Bytes[len=" + data.length + ", hex=" + preview + (data.length > previewSize ? "..." : "") + "]";
    }
}
