package io.partdb.raft.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

final class SnapshotFiles {

    private static final Logger log = LoggerFactory.getLogger(SnapshotFiles.class);
    private static final String STAGING_SUFFIX = ".tmp";
    private static final String INSTALL_SUFFIX = ".install";

    private final Path directory;

    private SnapshotFiles(Path directory) {
        this.directory = directory;
    }

    static SnapshotFiles open(Path directory) {
        try {
            Files.createDirectories(directory);
            Fsync.directory(directory.getParent());
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to open snapshot directory " + directory, e);
        }
        var files = new SnapshotFiles(directory);
        files.deleteAll(STAGING_SUFFIX);
        return files;
    }

    Path stagingFile(String prefix) {
        try {
            return Files.createTempFile(directory, prefix + "-", STAGING_SUFFIX);
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
        Fsync.directory(directory);
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
            log.atWarn().addKeyValue("path", path).setCause(e).log("Failed to delete snapshot file");
        }
    }

    private Path installPath(long index) {
        return directory.resolve("%016x%s".formatted(index, INSTALL_SUFFIX));
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
        Fsync.directory(directory);
    }
}
