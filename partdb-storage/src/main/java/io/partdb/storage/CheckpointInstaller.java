package io.partdb.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

final class CheckpointInstaller {

    private static final String BACKUP_SUFFIX = ".backup";
    private static final String MANIFEST_FILENAME = "MANIFEST";
    private static final String MANIFEST_TEMP_FILENAME = "MANIFEST.tmp";
    private static final String MANIFEST_BACKUP_FILENAME = MANIFEST_FILENAME + BACKUP_SUFFIX;

    private final Path directory;
    private final ManifestStore manifestStore;
    private final SstableStore sstableStore;

    CheckpointInstaller(Path directory, ManifestStore manifestStore, SstableStore sstableStore) {
        this.directory = directory;
        this.manifestStore = manifestStore;
        this.sstableStore = sstableStore;
    }

    void stageAndValidate(VersionCheckpoint checkpoint) throws IOException {
        checkpoint.stage(directory);
        checkpoint.validate(directory);
    }

    LoadedStoreVersion activate(VersionCheckpoint checkpoint) throws IOException {
        return checkpoint.activate(directory, manifestStore, sstableStore);
    }

    void cleanupStaged(VersionCheckpoint checkpoint) {
        checkpoint.cleanup(directory);
    }

    void backupCurrentState(SSTableManifest previousManifest) throws IOException {
        for (SSTableMetadata metadata : previousManifest.sstables()) {
            Path livePath = sstableStore.sstablePath(metadata.id());
            if (Files.exists(livePath)) {
                Files.move(
                    livePath,
                    backupPath(metadata.id()),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
                );
            }
        }

        Path manifestPath = directory.resolve(MANIFEST_FILENAME);
        if (Files.exists(manifestPath)) {
            Files.move(
                manifestPath,
                directory.resolve(MANIFEST_BACKUP_FILENAME),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        }

        Files.deleteIfExists(directory.resolve(MANIFEST_TEMP_FILENAME));
    }

    LoadedStoreVersion restoreLive(SSTableManifest manifest) {
        return sstableStore.loadState(manifest);
    }

    LoadedStoreVersion rollback(SSTableManifest previousManifest, boolean backedUpCurrentState) throws IOException {
        if (backedUpCurrentState) {
            rollbackRestoreFiles(previousManifest);
        }
        return sstableStore.loadState(previousManifest);
    }

    void cleanupBackups(SSTableManifest previousManifest) throws IOException {
        for (SSTableMetadata metadata : previousManifest.sstables()) {
            Files.deleteIfExists(backupPath(metadata.id()));
        }
        Files.deleteIfExists(directory.resolve(MANIFEST_BACKUP_FILENAME));
    }

    private void rollbackRestoreFiles(SSTableManifest previousManifest) throws IOException {
        deleteAllSSTables();

        for (SSTableMetadata metadata : previousManifest.sstables()) {
            Path backupPath = backupPath(metadata.id());
            if (Files.exists(backupPath)) {
                Files.move(
                    backupPath,
                    sstableStore.sstablePath(metadata.id()),
                    StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE
                );
            }
        }

        Files.deleteIfExists(directory.resolve(MANIFEST_FILENAME));
        Files.deleteIfExists(directory.resolve(MANIFEST_TEMP_FILENAME));

        Path manifestBackupPath = directory.resolve(MANIFEST_BACKUP_FILENAME);
        if (Files.exists(manifestBackupPath)) {
            Files.move(
                manifestBackupPath,
                directory.resolve(MANIFEST_FILENAME),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE
            );
        }
    }

    private void deleteAllSSTables() throws IOException {
        if (!Files.exists(directory)) {
            return;
        }

        try (Stream<Path> paths = Files.list(directory)) {
            for (Path path : paths
                .filter(path -> path.getFileName().toString().endsWith(".sst"))
                .toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private Path backupPath(long id) {
        return directory.resolve("%06d.sst%s".formatted(id, BACKUP_SUFFIX));
    }
}
