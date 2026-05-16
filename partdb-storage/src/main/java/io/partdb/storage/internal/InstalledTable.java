package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.util.Objects;

record InstalledTable(
    SSTableMetadata metadata,
    SSTableReader reader
) {

    InstalledTable {
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(reader, "reader");
        if (metadata.id() != reader.id()) {
            throw new IllegalArgumentException(
                "Installed table metadata id %d does not match reader id %d"
                    .formatted(metadata.id(), reader.id())
            );
        }
        if (metadata.level() != reader.level()) {
            throw new IllegalArgumentException(
                "Installed table metadata level %d does not match reader level %d"
                    .formatted(metadata.level(), reader.level())
            );
        }
    }
}
