package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

final class Slice implements Comparable<Slice> {

    static final Slice EMPTY = new Slice(MemorySegment.ofArray(new byte[0]));

    private final MemorySegment segment;
    private int hash;

    private Slice(MemorySegment segment) {
        if (segment.byteSize() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("slice too large: " + segment.byteSize());
        }
        this.segment = segment.isReadOnly() ? segment : segment.asReadOnly();
    }

    static Slice of(Bytes bytes) {
        return bytes.isEmpty() ? EMPTY : new Slice(MemorySegment.ofBuffer(bytes.asReadOnlyByteBuffer()));
    }

    static Slice wrap(byte[] bytes) {
        return bytes.length == 0 ? EMPTY : new Slice(MemorySegment.ofArray(bytes));
    }

    static Slice wrap(MemorySegment segment) {
        return segment.byteSize() == 0 ? EMPTY : new Slice(segment);
    }

    static Slice utf8(String value) {
        return wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    int length() {
        return (int) segment.byteSize();
    }

    MemorySegment segment() {
        return segment;
    }

    byte[] toByteArray() {
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }

    Bytes toBytes() {
        return Bytes.copyOf(toByteArray());
    }

    Slice copy() {
        return wrap(toByteArray());
    }

    @Override
    public int compareTo(Slice other) {
        long mismatch = segment.mismatch(other.segment);
        if (mismatch == -1) {
            return 0;
        }
        if (mismatch == segment.byteSize()) {
            return -1;
        }
        if (mismatch == other.segment.byteSize()) {
            return 1;
        }
        return Integer.compare(
            Byte.toUnsignedInt(segment.get(ValueLayout.JAVA_BYTE, mismatch)),
            Byte.toUnsignedInt(other.segment.get(ValueLayout.JAVA_BYTE, mismatch))
        );
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Slice other && segment.mismatch(other.segment) == -1;
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            for (long i = 0; i < segment.byteSize(); i++) {
                h = 31 * h + segment.get(ValueLayout.JAVA_BYTE, i);
            }
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        int preview = (int) Math.min(segment.byteSize(), 16);
        String hex = HexFormat.of().formatHex(segment.asSlice(0, preview).toArray(ValueLayout.JAVA_BYTE));
        return segment.byteSize() > preview ? "Slice[" + hex + "... " + segment.byteSize() + " bytes]" : "Slice[" + hex + "]";
    }
}
