package io.partdb.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class CatalogPersistence {

    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");

    private final Path directory;
    private final LsmConfig config;
    private final BlockCache cache;

    CatalogPersistence(Path directory, LsmConfig config, BlockCache cache) {
        this.directory = directory;
        this.config = config;
        this.cache = cache;
    }

    LoadedCatalog openState() {
        try {
            Files.createDirectories(directory);
            SSTableManifest manifest = SSTableManifest.readFrom(directory);
            List<Long> discoveredIds = discoverSSTableIds();
            validateManifestState(manifest, discoveredIds);
            return loadState(manifest);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open table catalog", e);
        }
    }

    LoadedCatalog loadState(SSTableManifest manifest) {
        return new LoadedCatalog(
            manifest,
            manifest.sstables().isEmpty() ? List.of() : loadReaders(manifest)
        );
    }

    void writeManifest(SSTableManifest manifest) {
        manifest.writeTo(directory);
    }

    SSTableWriter createWriter(long id, int level) {
        return SSTableWriter.create(id, level, sstablePath(id), config);
    }

    SSTableReader openReader(SSTableMetadata metadata) {
        return SSTableReader.open(metadata.id(), metadata.level(), sstablePath(metadata.id()), cache);
    }

    SSTableReader openCompactionReader(SSTableMetadata metadata) {
        return SSTableReader.open(metadata.id(), metadata.level(), sstablePath(metadata.id()), NoOpBlockCache.INSTANCE);
    }

    void delete(long id) throws IOException {
        Files.deleteIfExists(sstablePath(id));
    }

    Path sstablePath(long id) {
        return directory.resolve("%06d.sst".formatted(id));
    }

    Path directory() {
        return directory;
    }

    private List<SSTableReader> loadReaders(SSTableManifest manifest) {
        List<SSTableReader> readers = new ArrayList<>(manifest.sstables().size());
        for (SSTableMetadata metadata : manifest.sstables()) {
            readers.add(SSTableReader.open(metadata.id(), metadata.level(), sstablePath(metadata.id()), cache));
        }
        return readers;
    }

    private List<Long> discoverSSTableIds() throws IOException {
        if (!Files.exists(directory)) {
            return List.of();
        }

        try (Stream<Path> paths = Files.list(directory)) {
            return paths
                .map(path -> path.getFileName().toString())
                .map(SSTABLE_PATTERN::matcher)
                .filter(Matcher::matches)
                .map(matcher -> Long.parseLong(matcher.group(1)))
                .toList();
        }
    }

    private static void validateManifestState(SSTableManifest manifest, List<Long> discoveredIds) {
        if (manifest.sstables().isEmpty()) {
            if (!discoveredIds.isEmpty()) {
                throw new StorageException.Corruption(
                    "Found SSTables on disk without manifest entries"
                );
            }
            return;
        }

        Set<Long> manifestIds = manifest.sstables().stream()
            .map(SSTableMetadata::id)
            .collect(Collectors.toCollection(TreeSet::new));
        Set<Long> discoveredIdSet = new TreeSet<>(discoveredIds);

        if (!manifestIds.equals(discoveredIdSet)) {
            Set<Long> missingFromDisk = new TreeSet<>(manifestIds);
            missingFromDisk.removeAll(discoveredIdSet);

            Set<Long> untrackedOnDisk = new TreeSet<>(discoveredIdSet);
            untrackedOnDisk.removeAll(manifestIds);

            throw new StorageException.Corruption(
                "SSTable manifest does not match on-disk SSTables. missingFromDisk=%s, untrackedOnDisk=%s"
                    .formatted(missingFromDisk, untrackedOnDisk)
            );
        }
    }
}
