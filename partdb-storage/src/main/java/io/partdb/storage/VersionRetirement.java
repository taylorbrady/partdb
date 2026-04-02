package io.partdb.storage;

import java.util.List;
import java.util.Objects;

record VersionRetirement(
    SstableStore sstableStore,
    List<SSTableReader> readersToClose,
    List<SSTableMetadata> tablesToDelete
) {

    VersionRetirement {
        readersToClose = List.copyOf(Objects.requireNonNull(readersToClose, "readersToClose"));
        tablesToDelete = List.copyOf(Objects.requireNonNull(tablesToDelete, "tablesToDelete"));
        if (sstableStore == null && !tablesToDelete.isEmpty()) {
            throw new IllegalArgumentException("sstableStore is required when tablesToDelete is not empty");
        }
    }

    static VersionRetirement none() {
        return new VersionRetirement(null, List.of(), List.of());
    }

    boolean isEmpty() {
        return readersToClose.isEmpty() && tablesToDelete.isEmpty();
    }

    void execute() {
        if (!readersToClose.isEmpty()) {
            if (sstableStore != null) {
                sstableStore.closeReaders(readersToClose);
            } else {
                for (SSTableReader reader : readersToClose) {
                    reader.close();
                }
            }
        }
        if (!tablesToDelete.isEmpty()) {
            sstableStore.deleteTables(tablesToDelete);
        }
    }
}
