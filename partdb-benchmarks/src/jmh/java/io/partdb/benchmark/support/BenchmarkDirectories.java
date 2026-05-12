package io.partdb.benchmark.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

public final class BenchmarkDirectories {

    private BenchmarkDirectories() {
    }

    public static Path createTempDirectory(String prefix) throws IOException {
        return Files.createTempDirectory(prefix);
    }

    public static void deleteRecursively(Path directory) throws IOException {
        if (directory == null || !Files.exists(directory)) {
            return;
        }

        try (var paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder())
                .forEach(path -> {
                    try {
                        Files.deleteIfExists(path);
                    } catch (IOException e) {
                        throw new RuntimeIOException(e);
                    }
                });
        } catch (RuntimeIOException e) {
            throw e.cause;
        }
    }

    private static final class RuntimeIOException extends RuntimeException {
        private final IOException cause;

        private RuntimeIOException(IOException cause) {
            super(cause);
            this.cause = cause;
        }
    }
}
