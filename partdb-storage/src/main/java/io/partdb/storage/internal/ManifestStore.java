package io.partdb.storage.internal;

import io.partdb.storage.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

final class ManifestStore {

    private final Path directory;

    ManifestStore(Path directory) {
        this.directory = Objects.requireNonNull(directory, "directory");
    }

    SSTableManifest read() {
        try {
            Files.createDirectories(directory);
            return SSTableManifest.readFrom(directory);
        } catch (Exception e) {
            if (e instanceof StorageException storageException) {
                throw storageException;
            }
            throw new StorageException.IO("Failed to read manifest", e);
        }
    }

    void write(SSTableManifest manifest) {
        Objects.requireNonNull(manifest, "manifest");
        manifest.writeTo(directory);
    }

    Path directory() {
        return directory;
    }
}
