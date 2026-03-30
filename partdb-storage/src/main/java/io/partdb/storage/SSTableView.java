package io.partdb.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

final class SSTableView implements AutoCloseable {

    private final SSTableSetRef ref;
    private final Manifest manifest;
    private final int maxLevel;
    private final Map<Long, SSTable> readersById;

    SSTableView(SSTableSetRef ref, Manifest manifest) {
        this.ref = ref;
        this.manifest = manifest;
        this.maxLevel = manifest.maxLevel();
        this.readersById = indexReaders(ref.readers());
    }

    List<SSTable> level0() {
        return level(0);
    }

    List<SSTable> level(int level) {
        return readersFor(manifest.level(level));
    }

    Optional<Mutation> get(Slice key) {
        Objects.requireNonNull(key, "key");

        Optional<Mutation> level0 = getFrom(manifest.level(0), key);
        if (level0.isPresent()) {
            return level0;
        }

        for (int level = 1; level <= maxLevel; level++) {
            Optional<Mutation> result = getFrom(manifest.level(level), key);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    List<SSTable> scanTables(Slice startKey, Slice endKey) {
        List<SSTable> tables = new ArrayList<>(manifest.sstables().size());

        for (SSTableDescriptor descriptor : manifest.level(0)) {
            if (descriptor.overlaps(startKey, endKey)) {
                tables.add(readerFor(descriptor));
            }
        }

        for (int level = 1; level <= maxLevel; level++) {
            for (SSTableDescriptor descriptor : manifest.level(level)) {
                if (descriptor.overlaps(startKey, endKey)) {
                    tables.add(readerFor(descriptor));
                }
            }
        }

        return List.copyOf(tables);
    }

    int maxLevel() {
        return maxLevel;
    }

    Manifest manifest() {
        return manifest;
    }

    List<SSTable> all() {
        return readersFor(manifest.sstables());
    }

    @Override
    public void close() {
        ref.release();
    }

    private Optional<Mutation> getFrom(List<SSTableDescriptor> descriptors, Slice key) {
        for (SSTableDescriptor descriptor : descriptors) {
            if (!descriptor.mightContain(key)) {
                continue;
            }

            Optional<Mutation> result = readerFor(descriptor).get(key);
            if (result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }

    private List<SSTable> readersFor(List<SSTableDescriptor> descriptors) {
        List<SSTable> readers = new ArrayList<>(descriptors.size());
        for (SSTableDescriptor descriptor : descriptors) {
            readers.add(readerFor(descriptor));
        }
        return List.copyOf(readers);
    }

    private SSTable readerFor(SSTableDescriptor descriptor) {
        SSTable reader = readersById.get(descriptor.id());
        if (reader == null) {
            throw new StorageException.Corruption(
                "Missing SSTable reader for descriptor id " + descriptor.id()
            );
        }
        return reader;
    }

    private static Map<Long, SSTable> indexReaders(List<SSTable> readers) {
        Map<Long, SSTable> readersById = new HashMap<>(readers.size());
        for (SSTable reader : readers) {
            readersById.put(reader.id(), reader);
        }
        return Map.copyOf(readersById);
    }
}
