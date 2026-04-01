package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public record EntryRecord(Bytes key, Bytes value, Revision modRevision) {

    public EntryRecord {
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
        modRevision = Objects.requireNonNull(modRevision, "modRevision must not be null");
    }
}
