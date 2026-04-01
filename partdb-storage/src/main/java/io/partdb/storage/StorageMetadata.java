package io.partdb.storage;

import java.util.Objects;

public record StorageMetadata(Revision appliedThrough) {

    public StorageMetadata {
        appliedThrough = Objects.requireNonNull(appliedThrough, "appliedThrough must not be null");
    }
}
