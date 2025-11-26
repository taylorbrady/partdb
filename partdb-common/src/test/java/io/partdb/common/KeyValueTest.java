package io.partdb.common;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class KeyValueTest {

    @Test
    void constructorCreatesInstanceWithKeyAndValue() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        KeyValue pair = new KeyValue(key, value);

        assertThat(pair.key()).isEqualTo(key);
        assertThat(pair.value()).isEqualTo(value);
    }

    @Test
    void constructorThrowsWhenKeyIsNull() {
        ByteArray value = ByteArray.of((byte) 1);

        assertThatThrownBy(() -> new KeyValue(null, value))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("key cannot be null");
    }

    @Test
    void constructorThrowsWhenValueIsNull() {
        ByteArray key = ByteArray.of((byte) 1);

        assertThatThrownBy(() -> new KeyValue(key, null))
            .isInstanceOf(NullPointerException.class)
            .hasMessageContaining("value cannot be null");
    }

    @Test
    void equalsPairsWithSameContent() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value = ByteArray.of((byte) 2);

        KeyValue pair1 = new KeyValue(key, value);
        KeyValue pair2 = new KeyValue(key, value);

        assertThat(pair1).isEqualTo(pair2);
        assertThat(pair1.hashCode()).isEqualTo(pair2.hashCode());
    }

    @Test
    void notEqualWhenKeyDiffers() {
        ByteArray key1 = ByteArray.of((byte) 1);
        ByteArray key2 = ByteArray.of((byte) 2);
        ByteArray value = ByteArray.of((byte) 3);

        KeyValue pair1 = new KeyValue(key1, value);
        KeyValue pair2 = new KeyValue(key2, value);

        assertThat(pair1).isNotEqualTo(pair2);
    }

    @Test
    void notEqualWhenValueDiffers() {
        ByteArray key = ByteArray.of((byte) 1);
        ByteArray value1 = ByteArray.of((byte) 2);
        ByteArray value2 = ByteArray.of((byte) 3);

        KeyValue pair1 = new KeyValue(key, value1);
        KeyValue pair2 = new KeyValue(key, value2);

        assertThat(pair1).isNotEqualTo(pair2);
    }

    @Test
    void toStringIncludesKeyAndValue() {
        ByteArray key = ByteArray.of((byte) 0x0A);
        ByteArray value = ByteArray.of((byte) 0x0B);

        KeyValue pair = new KeyValue(key, value);

        assertThat(pair.toString()).contains("0a").contains("0b");
    }
}
