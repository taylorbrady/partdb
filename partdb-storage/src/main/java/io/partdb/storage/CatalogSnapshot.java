package io.partdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

final class CatalogSnapshot implements AutoCloseable {

    private final CatalogGeneration.CatalogLease lease;
    private final SSTableManifest manifest;
    private final int maxLevel;

    CatalogSnapshot(CatalogGeneration.CatalogLease lease) {
        this.lease = lease;
        this.manifest = lease.manifest();
        this.maxLevel = manifest.maxLevel();
    }

    List<SSTableReader> level0() {
        return level(0);
    }

    List<SSTableReader> level(int level) {
        return readersFor(manifest.level(level));
    }

    Optional<StoredEntry> get(Slice key) {
        Objects.requireNonNull(key, "key");

        Optional<StoredEntry> level0 = getFrom(manifest.level(0), key);
        if (level0.isPresent()) {
            return level0;
        }

        for (int level = 1; level <= maxLevel; level++) {
            Optional<StoredEntry> result = getFrom(manifest.level(level), key);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
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
        lease.close();
    }

    private Optional<StoredEntry> getFrom(List<SSTableMetadata> metadata, Slice key) {
        for (SSTableMetadata table : metadata) {
            if (!table.mightContain(key)) {
                continue;
            }

            Optional<StoredEntry> result = readerFor(table).get(key);
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
