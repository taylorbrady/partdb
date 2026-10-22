package io.partdb.raft.runtime;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Map.entry;

final class SnapshotFiles {

    private static final Logger log = System.getLogger(SnapshotFiles.class.getName());
    private static final String INSTALL_SUFFIX = ".install";

    private final Path directory;

    private SnapshotFiles(Path directory) {
        this.directory = directory;
    }

    static SnapshotFiles open(Path directory) {
        try {
            Files.createDirectories(directory);
            Disk.syncDirectory(directory.getParent());
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to open snapshot directory " + directory, e);
        }
        var files = new SnapshotFiles(directory);
        files.deleteAll(Disk.TEMPORARY_SUFFIX);
        return files;
    }

    Path stagingFile(String prefix) {
        try {
            return Files.createTempFile(directory, prefix + "-", Disk.TEMPORARY_SUFFIX);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to create a snapshot staging file in " + directory, e);
        }
    }

    Path promote(Path staged, long index) {
        Path installing = installPath(index);
        try {
            Files.move(staged, installing, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to stage snapshot " + index + " for install", e);
        }
        Disk.syncDirectory(directory);
        return installing;
    }

    Optional<Path> installing(long index) {
        Path installing = installPath(index);
        return Files.exists(installing) ? Optional.of(installing) : Optional.empty();
    }

    void clearInstalls() {
        deleteAll(INSTALL_SUFFIX);
    }

    static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.log(Level.WARNING, "Failed to delete snapshot file", entry("path", path), e);
        }
    }

    private Path installPath(long index) {
        return directory.resolve("%06d%s".formatted(index, INSTALL_SUFFIX));
    }

    private void deleteAll(String suffix) {
        List<Path> matching;
        try (Stream<Path> files = Files.list(directory)) {
            matching = files.filter(path -> path.getFileName().toString().endsWith(suffix)).toList();
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to list snapshot files in " + directory, e);
        }
        if (matching.isEmpty()) {
            return;
        }
        try {
            for (Path path : matching) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to delete snapshot files in " + directory, e);
        }
        Disk.syncDirectory(directory);
    }
}
