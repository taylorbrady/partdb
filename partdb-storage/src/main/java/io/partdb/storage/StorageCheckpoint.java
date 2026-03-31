package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record StorageCheckpoint(Bytes bytes) {

    public StorageCheckpoint {
        bytes = Objects.requireNonNull(bytes, "bytes must not be null");
    }
}
