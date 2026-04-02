package io.partdb.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

sealed interface VersionEdit permits VersionEdit.Flush, VersionEdit.Compaction {

    List<InstalledTable> additions();

    List<SSTableMetadata> removals();

    AdditionMode additionMode();

    long durableThroughRevision(SSTableManifest currentManifest);

    default SSTableManifest applyTo(SSTableManifest currentManifest, long nextSstableId) {
        Objects.requireNonNull(currentManifest, "currentManifest");

        List<SSTableMetadata> updated = new ArrayList<>(currentManifest.sstables());
        if (!removals().isEmpty()) {
            updated.removeAll(removals());
        }
        if (!additions().isEmpty()) {
            List<SSTableMetadata> addedMetadata = additions().stream().map(InstalledTable::metadata).toList();
            if (additionMode() == AdditionMode.PREPEND) {
                updated.addAll(0, addedMetadata);
            } else {
                updated.addAll(addedMetadata);
            }
        }

        return new SSTableManifest(nextSstableId, durableThroughRevision(currentManifest), updated);
    }

    default Set<Long> removedIds() {
        return removals().stream().map(SSTableMetadata::id).collect(Collectors.toSet());
    }

    enum AdditionMode {
        PREPEND,
        APPEND
    }

    record Flush(InstalledTable addition, long durableThroughRevision) implements VersionEdit {
        public Flush {
            Objects.requireNonNull(addition, "addition");
            if (durableThroughRevision < 0) {
                throw new IllegalArgumentException("durableThroughRevision must be non-negative");
            }
        }

        @Override
        public List<InstalledTable> additions() {
            return List.of(addition);
        }

        @Override
        public List<SSTableMetadata> removals() {
            return List.of();
        }

        @Override
        public AdditionMode additionMode() {
            return AdditionMode.PREPEND;
        }

        @Override
        public long durableThroughRevision(SSTableManifest currentManifest) {
            Objects.requireNonNull(currentManifest, "currentManifest");
            return durableThroughRevision;
        }
    }

    record Compaction(List<SSTableMetadata> removals, List<InstalledTable> additions) implements VersionEdit {
        public Compaction {
            removals = List.copyOf(Objects.requireNonNull(removals, "removals"));
            additions = List.copyOf(Objects.requireNonNull(additions, "additions"));
        }

        @Override
        public AdditionMode additionMode() {
            return AdditionMode.APPEND;
        }

        @Override
        public long durableThroughRevision(SSTableManifest currentManifest) {
            return currentManifest.durableThroughRevision();
        }
    }
}
