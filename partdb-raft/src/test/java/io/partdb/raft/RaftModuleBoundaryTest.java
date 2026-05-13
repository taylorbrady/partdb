package io.partdb.raft;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class RaftModuleBoundaryTest {

    private static final List<String> FORBIDDEN_IMPORT_PREFIXES = List.of(
        "import java.io.",
        "import java.nio.",
        "import java.net.",
        "import java.time.",
        "import java.util.concurrent.",
        "import io.grpc.",
        "import io.partdb.cluster.",
        "import io.partdb.consensus.",
        "import io.partdb.storage.",
        "import io.partdb.transport."
    );

    private static final List<String> FORBIDDEN_RUNTIME_REFERENCES = List.of(
        "System.currentTimeMillis(",
        "System.nanoTime(",
        "new Thread(",
        "Thread.of",
        "Executors."
    );

    @Test
    void mainSourcesDoNotDependOnRuntimeConcerns() throws IOException {
        Path sourceRoot = sourceRoot();
        try (var files = Files.walk(sourceRoot)) {
            var violations = files
                .filter(path -> path.toString().endsWith(".java"))
                .flatMap(path -> violations(path).stream())
                .toList();

            assertTrue(violations.isEmpty(), () -> "Forbidden partdb-raft dependency:\n" + String.join("\n", violations));
        }
    }

    private static List<String> violations(Path path) {
        try {
            String source = Files.readString(path);
            var violations = new java.util.ArrayList<String>();

            for (String prefix : FORBIDDEN_IMPORT_PREFIXES) {
                if (source.contains(prefix)) {
                    violations.add(path + " imports " + prefix.substring("import ".length()));
                }
            }

            for (String reference : FORBIDDEN_RUNTIME_REFERENCES) {
                if (source.contains(reference)) {
                    violations.add(path + " references " + reference);
                }
            }

            return violations;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static Path sourceRoot() {
        Path cwd = Path.of(System.getProperty("user.dir"));
        Path moduleLocal = cwd.resolve("src/main/java/io/partdb/raft");
        if (Files.isDirectory(moduleLocal)) {
            return moduleLocal;
        }
        return cwd.resolve("partdb-raft/src/main/java/io/partdb/raft");
    }
}
