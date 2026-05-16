package io.partdb.storage.internal;

import io.partdb.storage.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

final class SSTableStore {

    private static final Logger log = LoggerFactory.getLogger(SSTableStore.class);
    private static final Pattern SSTABLE_PATTERN = Pattern.compile("(\\d{6})\\.sst");

    private final Path directory;
    private final LsmConfig config;
    private final BlockCache cache;

    SSTableStore(Path directory, LsmConfig config, BlockCache cache) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.config = Objects.requireNonNull(config, "config");
        this.cache = Objects.requireNonNull(cache, "cache");
    }

    LoadedStoreVersion openState(ManifestStore manifestStore) {
        Objects.requireNonNull(manifestStore, "manifestStore");
        try {
            Files.createDirectories(directory);
            SSTableManifest manifest = manifestStore.read();
            List<Long> discoveredIds = discoverSSTableIds();
            validateManifestState(manifest, discoveredIds);
            return loadState(manifest);
        } catch (IOException e) {
            throw new StorageException.IO("Failed to open SSTable store", e);
        }
    }

    LoadedStoreVersion loadState(SSTableManifest manifest) {
        return new LoadedStoreVersion(
            manifest,
            manifest.sstables().isEmpty() ? List.of() : loadReaders(manifest)
        );
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

    List<SSTableReader> openReaders(List<SSTableMetadata> metadata) {
        List<SSTableReader> readers = new ArrayList<>(metadata.size());
        try {
            for (SSTableMetadata table : metadata) {
                readers.add(openReader(table));
            }
            return readers;
        } catch (RuntimeException e) {
            closeReaders(readers);
            throw e;
        }
    }

    void delete(long id) throws IOException {
        Files.deleteIfExists(sstablePath(id));
    }

    void deleteTables(List<SSTableMetadata> metadata) {
        for (SSTableMetadata table : metadata) {
            try {
                delete(table.id());
            } catch (IOException e) {
                log.atWarn()
                    .setCause(e)
                    .addKeyValue("sstableId", table.id())
                    .log("Failed to delete SSTable");
            }
        }
    }

    void closeReaders(List<SSTableReader> readers) {
        for (SSTableReader reader : readers) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                log.atWarn()
                    .setCause(e)
                    .addKeyValue("sstableId", reader.id())
                    .log("Failed to close SSTable reader");
            }
        }
    }

    Path sstablePath(long id) {
        return directory.resolve("%06d.sst".formatted(id));
    }

    private List<SSTableReader> loadReaders(SSTableManifest manifest) {
        return openReaders(manifest.sstables());
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
