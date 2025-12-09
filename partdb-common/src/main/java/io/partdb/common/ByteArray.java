package io.partdb.common;

import java.util.Arrays;
import java.util.HexFormat;

public final class ByteArray implements Comparable<ByteArray> {

    private static final ByteArray EMPTY = new ByteArray(new byte[0]);

    private final byte[] data;

    private ByteArray(byte[] data) {
        this.data = data;
    }

    public static ByteArray of(byte... bytes) {
        if (bytes.length == 0) {
            return EMPTY;
        }
        return new ByteArray(bytes.clone());
    }

    public static ByteArray copyOf(byte[] bytes) {
        if (bytes.length == 0) {
            return EMPTY;
        }
        return new ByteArray(bytes.clone());
    }

    public static ByteArray copyOf(byte[] bytes, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > bytes.length) {
            throw new IndexOutOfBoundsException(
                "Invalid offset=%d, length=%d for array of size %d".formatted(offset, length, bytes.length)
            );
        }
        if (length == 0) {
            return EMPTY;
        }
        return new ByteArray(Arrays.copyOfRange(bytes, offset, offset + length));
    }

    public static ByteArray empty() {
        return EMPTY;
    }

    public int length() {
        return data.length;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    public byte get(int index) {
        return data[index];
    }

    public ByteArray slice(int offset, int length) {
        return copyOf(data, offset, length);
    }

    public byte[] toByteArray() {
        return data.clone();
    }

    @Override
    public int compareTo(ByteArray other) {
        return Arrays.compareUnsigned(data, other.data);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ByteArray other && Arrays.equals(data, other.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        if (data.length == 0) {
            return "ByteArray[]";
        }
        String hex = HexFormat.of().formatHex(data);
        if (hex.length() > 32) {
            return "ByteArray[" + hex.substring(0, 32) + "... (" + data.length + " bytes)]";
        }
        return "ByteArray[" + hex + "]";
    }
}
