package io.partdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class ByteArrayTest {

    @Test
    void wrapCreatesInstanceWithFullArray() {
        byte[] data = {1, 2, 3, 4, 5};
        ByteArray ba = ByteArray.wrap(data);

        assertThat(ba.size()).isEqualTo(5);
        assertThat(ba.get(0)).isEqualTo((byte) 1);
        assertThat(ba.get(4)).isEqualTo((byte) 5);
    }

    @Test
    void wrapWithOffsetAndLength() {
        byte[] data = {1, 2, 3, 4, 5};
        ByteArray ba = ByteArray.wrap(data, 1, 3);

        assertThat(ba.size()).isEqualTo(3);
        assertThat(ba.get(0)).isEqualTo((byte) 2);
        assertThat(ba.get(2)).isEqualTo((byte) 4);
    }

    @Test
    void wrapWithInvalidOffsetThrows() {
        byte[] data = {1, 2, 3};
        assertThatThrownBy(() -> ByteArray.wrap(data, -1, 2))
            .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ByteArray.wrap(data, 2, 5))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void emptyReturnsEmptyInstance() {
        ByteArray empty = ByteArray.empty();
        assertThat(empty.size()).isZero();
        assertThat(empty.isEmpty()).isTrue();
    }

    @Test
    void wrapEmptyArrayReturnsSameInstance() {
        ByteArray ba1 = ByteArray.wrap(new byte[0]);
        ByteArray ba2 = ByteArray.empty();
        assertThat(ba1).isSameAs(ba2);
    }

    @Test
    void copyOfCreatesDefensiveCopy() {
        byte[] original = {1, 2, 3};
        ByteArray ba = ByteArray.copyOf(original);

        original[0] = 99;

        assertThat(ba.get(0)).isEqualTo((byte) 1);
    }

    @Test
    void ofVarargsCreatesInstance() {
        ByteArray ba = ByteArray.of((byte) 1, (byte) 2, (byte) 3);

        assertThat(ba.size()).isEqualTo(3);
        assertThat(ba.get(0)).isEqualTo((byte) 1);
        assertThat(ba.get(2)).isEqualTo((byte) 3);
    }

    @Test
    void sliceCreatesSubRange() {
        ByteArray ba = ByteArray.of((byte) 1, (byte) 2, (byte) 3, (byte) 4, (byte) 5);
        ByteArray slice = ba.slice(1, 3);

        assertThat(slice.size()).isEqualTo(3);
        assertThat(slice.get(0)).isEqualTo((byte) 2);
        assertThat(slice.get(2)).isEqualTo((byte) 4);
    }

    @Test
    void sliceWithInvalidRangeThrows() {
        ByteArray ba = ByteArray.of((byte) 1, (byte) 2, (byte) 3);
        assertThatThrownBy(() -> ba.slice(-1, 1))
            .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ba.slice(1, 10))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getWithInvalidIndexThrows() {
        ByteArray ba = ByteArray.of((byte) 1, (byte) 2);
        assertThatThrownBy(() -> ba.get(-1))
            .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ba.get(2))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void toByteArrayCreatesDefensiveCopy() {
        ByteArray ba = ByteArray.of((byte) 1, (byte) 2, (byte) 3);
        byte[] array = ba.toByteArray();

        array[0] = 99;

        assertThat(ba.get(0)).isEqualTo((byte) 1);
    }

    @Test
    void equalsReturnsTrueForSameContent() {
        ByteArray ba1 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);

        assertThat(ba1).isEqualTo(ba2);
        assertThat(ba1.hashCode()).isEqualTo(ba2.hashCode());
    }

    @Test
    void equalsReturnsFalseForDifferentContent() {
        ByteArray ba1 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 2, (byte) 4);

        assertThat(ba1).isNotEqualTo(ba2);
    }

    @Test
    void equalsWorksWithSlices() {
        byte[] data = {0, 1, 2, 3, 4, 5};
        ByteArray ba1 = ByteArray.wrap(data, 1, 3);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);

        assertThat(ba1).isEqualTo(ba2);
    }

    @Test
    void compareToReturnsZeroForEqual() {
        ByteArray ba1 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);

        assertThat(ba1.compareTo(ba2)).isZero();
    }

    @Test
    void compareToReturnsNegativeWhenLess() {
        ByteArray ba1 = ByteArray.of((byte) 1, (byte) 2);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 3);

        assertThat(ba1.compareTo(ba2)).isNegative();
    }

    @Test
    void compareToReturnsPositiveWhenGreater() {
        ByteArray ba1 = ByteArray.of((byte) 1, (byte) 3);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 2);

        assertThat(ba1.compareTo(ba2)).isPositive();
    }

    @Test
    void compareToHandlesDifferentLengths() {
        ByteArray ba1 = ByteArray.of((byte) 1, (byte) 2);
        ByteArray ba2 = ByteArray.of((byte) 1, (byte) 2, (byte) 3);

        assertThat(ba1.compareTo(ba2)).isNegative();
        assertThat(ba2.compareTo(ba1)).isPositive();
    }

    @Test
    void compareToWorksWithUnsignedBytes() {
        ByteArray ba1 = ByteArray.of((byte) 0xFF);
        ByteArray ba2 = ByteArray.of((byte) 0x01);

        assertThat(ba1.compareTo(ba2)).isPositive();
    }

    @Test
    void toStringShowsHexRepresentation() {
        ByteArray ba = ByteArray.of((byte) 0x01, (byte) 0x02, (byte) 0xFF);
        String str = ba.toString();

        assertThat(str).contains("0102ff");
    }

    @Test
    void toStringTruncatesLongArrays() {
        byte[] data = new byte[50];
        ByteArray ba = ByteArray.wrap(data);
        String str = ba.toString();

        assertThat(str).contains("...");
        assertThat(str).contains("50 bytes");
    }

    @Test
    void emptyToString() {
        assertThat(ByteArray.empty().toString()).isEqualTo("ByteArray[]");
    }
}
