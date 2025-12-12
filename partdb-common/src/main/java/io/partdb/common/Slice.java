package io.partdb.common;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

public final class Slice implements Comparable<Slice> {

    private static final Slice EMPTY = new Slice(MemorySegment.ofArray(new byte[0]));

    private final MemorySegment segment;
    private int cachedHash;

    private Slice(MemorySegment segment) {
        if (segment.byteSize() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Slice too large: " + segment.byteSize());
        }
        this.segment = segment.isReadOnly() ? segment : segment.asReadOnly();
    }

    public static Slice of(byte[] bytes) {
        if (bytes.length == 0) {
            return EMPTY;
        }
        return new Slice(MemorySegment.ofArray(bytes.clone()));
    }

    public static Slice of(String s) {
        if (s.isEmpty()) {
            return EMPTY;
        }
        return of(s.getBytes(StandardCharsets.UTF_8));
    }

    public static Slice of(ByteBuffer buffer) {
        if (buffer.remaining() == 0) {
            return EMPTY;
        }
        return new Slice(MemorySegment.ofBuffer(buffer));
    }

    public static Slice wrap(MemorySegment segment) {
        if (segment.byteSize() == 0) {
            return EMPTY;
        }
        return new Slice(segment);
    }

    public static Slice empty() {
        return EMPTY;
    }

    public int length() {
        return (int) segment.byteSize();
    }

    public boolean isEmpty() {
        return segment.byteSize() == 0;
    }

    public MemorySegment segment() {
        return segment;
    }

    public byte[] toByteArray() {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    public ByteBuffer asByteBuffer() {
        return segment.asByteBuffer();
    }

    public Slice slice(long offset, long length) {
        if (length == 0) {
            return EMPTY;
        }
        return new Slice(segment.asSlice(offset, length));
    }

    public boolean startsWith(Slice prefix) {
        if (prefix.length() > length()) {
            return false;
        }
        return segment.asSlice(0, prefix.length()).mismatch(prefix.segment) == -1;
    }

    @Override
    public int compareTo(Slice other) {
        long mismatch = segment.mismatch(other.segment);
        if (mismatch == -1) {
            return 0;
        }
        long len1 = segment.byteSize();
        long len2 = other.segment.byteSize();
        if (mismatch == len1) {
            return -1;
        }
        if (mismatch == len2) {
            return 1;
        }
        int b1 = Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, mismatch));
        int b2 = Byte.toUnsignedInt(other.segment.get(ValueLayout.JAVA_BYTE, mismatch));
        return b1 - b2;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Slice other)) {
            return false;
        }
        return segment.mismatch(other.segment) == -1;
    }

    @Override
    public int hashCode() {
        int h = cachedHash;
        if (h == 0 && segment.byteSize() > 0) {
            long size = segment.byteSize();
            for (long i = 0; i < size; i++) {
                h = 31 * h + segment.get(ValueLayout.JAVA_BYTE, i);
            }
            cachedHash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        long size = segment.byteSize();
        if (size == 0) {
            return "Slice[]";
        }
        int previewLen = (int) Math.min(size, 16);
        byte[] preview = segment.asSlice(0, previewLen).toArray(ValueLayout.JAVA_BYTE);
        String hex = HexFormat.of().formatHex(preview);
        if (size > 16) {
            return "Slice[" + hex + "... (" + size + " bytes)]";
        }
        return "Slice[" + hex + "]";
    }
}
