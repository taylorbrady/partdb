package io.partdb.storage;

import java.util.Set;
import java.util.List;
import java.util.stream.Collectors;

final class SSTableView implements AutoCloseable {

    private final SSTableSetRef ref;
    private final Manifest manifest;
    private final int maxLevel;

    SSTableView(SSTableSetRef ref, Manifest manifest) {
        this.ref = ref;
        this.manifest = manifest;
        this.maxLevel = manifest.maxLevel();
    }

    List<SSTable> level0() {
        return level(0);
    }

    List<SSTable> level(int level) {
        Set<Long> levelIds = manifest.level(level).stream()
            .map(SSTableDescriptor::id)
            .collect(Collectors.toSet());

        return ref.readers().stream()
            .filter(sst -> levelIds.contains(sst.id()))
            .toList();
    }

    int maxLevel() {
        return maxLevel;
    }

    Manifest manifest() {
        return manifest;
    }

    List<SSTable> all() {
        return ref.readers();
    }

    @Override
    public void close() {
        ref.release();
    }
}
