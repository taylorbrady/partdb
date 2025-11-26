package io.partdb.storage;

import io.partdb.common.ByteArray;

public record StoreEntry(ByteArray key, ByteArray value, boolean tombstone) {

    public static StoreEntry of(ByteArray key, ByteArray value) {
        return new StoreEntry(key, value, false);
    }

    public static StoreEntry tombstone(ByteArray key) {
        return new StoreEntry(key, null, true);
    }
}
