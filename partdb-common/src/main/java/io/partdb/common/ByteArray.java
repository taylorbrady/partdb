package io.partdb.common;

import java.util.Arrays;
import java.util.HexFormat;

public final class ByteArray implements Comparable<ByteArray> {

    private static final ByteArray EMPTY = new ByteArray(new byte[0], 0, 0);

    private final byte[] data;
    private final int offset;
    private final int length;

    private ByteArray(byte[] data, int offset, int length) {
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public static ByteArray wrap(byte[] data) {
        if (data.length == 0) {
            return EMPTY;
        }
        return new ByteArray(data, 0, data.length);
    }

    public static ByteArray wrap(byte[] data, int offset, int length) {
        if (offset < 0 || length < 0 || offset + length > data.length) {
            throw new IndexOutOfBoundsException(
                "Invalid offset=%d, length=%d for array of size %d".formatted(offset, length, data.length)
            );
        }
        if (length == 0) {
            return EMPTY;
        }
        return new ByteArray(data, offset, length);
    }

    public static ByteArray copyOf(byte[] data) {
        if (data.length == 0) {
            return EMPTY;
        }
        return new ByteArray(Arrays.copyOf(data, data.length), 0, data.length);
    }

    public static ByteArray of(byte... bytes) {
        return copyOf(bytes);
    }

    public static ByteArray empty() {
        return EMPTY;
    }

    public int size() {
        return length;
    }

    public boolean isEmpty() {
        return length == 0;
    }

    public byte get(int index) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + length);
        }
        return data[offset + index];
    }

    public ByteArray slice(int sliceOffset, int sliceLength) {
        if (sliceOffset < 0 || sliceLength < 0 || sliceOffset + sliceLength > length) {
            throw new IndexOutOfBoundsException(
                "Invalid slice offset=%d, length=%d for ByteArray of size %d".formatted(sliceOffset, sliceLength, length)
            );
        }
        if (sliceLength == 0) {
            return EMPTY;
        }
        return new ByteArray(data, offset + sliceOffset, sliceLength);
    }

    public byte[] toByteArray() {
        return Arrays.copyOfRange(data, offset, offset + length);
    }

    @Override
    public int compareTo(ByteArray other) {
        return Arrays.compareUnsigned(
            data, offset, offset + length,
            other.data, other.offset, other.offset + other.length
        );
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ByteArray other &&
               Arrays.equals(data, offset, offset + length,
                           other.data, other.offset, other.offset + other.length);
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + data[i];
        }
        return result;
    }

    @Override
    public String toString() {
        if (length == 0) {
            return "ByteArray[]";
        }
        String hex = HexFormat.of().formatHex(data, offset, offset + length);
        if (hex.length() > 32) {
            return "ByteArray[" + hex.substring(0, 32) + "... (" + length + " bytes)]";
        }
        return "ByteArray[" + hex + "]";
    }
}
