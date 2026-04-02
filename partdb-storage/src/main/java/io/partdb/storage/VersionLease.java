package io.partdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

final class VersionLease implements AutoCloseable {

    private final StoreVersion.Lease lease;
    private final Runnable onClose;
    private final SSTableManifest manifest;
    private final int maxLevel;

    VersionLease(StoreVersion.Lease lease) {
        this(lease, () -> {});
    }

    VersionLease(StoreVersion.Lease lease, Runnable onClose) {
        this.lease = lease;
        this.onClose = Objects.requireNonNull(onClose, "onClose");
        this.manifest = lease.manifest();
        this.maxLevel = manifest.maxLevel();
    }

    List<SSTableReader> level0() {
        return level(0);
    }

    List<SSTableReader> level(int level) {
        return readersFor(manifest.level(level));
    }

    Optional<StoredEntry> get(Slice key, long snapshotRevision) {
        Objects.requireNonNull(key, "key");

        Optional<StoredEntry> level0 = getFrom(manifest.level(0), key, snapshotRevision);
        if (level0.isPresent()) {
            return level0;
        }

        for (int level = 1; level <= maxLevel; level++) {
            Optional<StoredEntry> result = getFrom(manifest.level(level), key, snapshotRevision);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    Optional<StoredEntry> get(Slice key) {
        return get(key, Long.MAX_VALUE);
    }

    List<SSTableReader> scanTables(ScanBounds bounds) {
        List<SSTableReader> tables = new ArrayList<>(manifest.sstables().size());

        for (SSTableMetadata metadata : manifest.level(0)) {
            if (metadata.overlaps(bounds)) {
                tables.add(readerFor(metadata));
            }
        }

        for (int level = 1; level <= maxLevel; level++) {
            for (SSTableMetadata metadata : manifest.level(level)) {
                if (metadata.overlaps(bounds)) {
                    tables.add(readerFor(metadata));
                }
            }
        }

        return List.copyOf(tables);
    }

    int maxLevel() {
        return maxLevel;
    }

    SSTableManifest manifest() {
        return manifest;
    }

    List<SSTableReader> all() {
        return readersFor(manifest.sstables());
    }

    @Override
    public void close() {
        try {
            lease.close();
        } finally {
            onClose.run();
        }
    }

    private Optional<StoredEntry> getFrom(List<SSTableMetadata> metadata, Slice key, long snapshotRevision) {
        for (SSTableMetadata table : metadata) {
            if (!table.mightContain(key)) {
                continue;
            }

            Optional<StoredEntry> result = readerFor(table).get(key, snapshotRevision);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    private List<SSTableReader> readersFor(List<SSTableMetadata> metadata) {
        List<SSTableReader> readers = new ArrayList<>(metadata.size());
        for (SSTableMetadata table : metadata) {
            readers.add(readerFor(table));
        }
        return List.copyOf(readers);
    }

    private SSTableReader readerFor(SSTableMetadata metadata) {
        return lease.readerFor(metadata.id());
    }
}
