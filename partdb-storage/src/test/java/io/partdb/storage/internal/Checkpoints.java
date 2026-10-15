package io.partdb.storage.internal;

import io.partdb.storage.StorageEngine;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

final class Checkpoints {
    private Checkpoints() {
    }

    static byte[] capture(StorageEngine store) {
        var out = new ByteArrayOutputStream();
        store.checkpoint(out);
        return out.toByteArray();
    }

    static void restore(StorageEngine store, byte[] checkpoint) {
        store.restore(new ByteArrayInputStream(checkpoint));
    }
}
