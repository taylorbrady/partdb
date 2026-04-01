package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record ValueRecord(Bytes value, Revision modRevision) {

    public ValueRecord {
        value = Objects.requireNonNull(value, "value must not be null");
        modRevision = Objects.requireNonNull(modRevision, "modRevision must not be null");
    }
}
