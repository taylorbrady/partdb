package io.partdb.server;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

class ServerModuleBoundaryTest {

    private static final List<String> FORBIDDEN_IMPORT_PREFIXES = List.of(
        "import io.grpc.",
        "import io.partdb.app.",
        "import io.partdb.client.",
        "import io.partdb.grpc.",
        "import io.partdb.raft.",
        "import io.partdb.storage."
    );

    @Test
    void mainSourcesStayAtProcessAssemblyBoundary() throws IOException {
        Path sourceRoot = sourceRoot();
        try (var files = Files.walk(sourceRoot)) {
            var violations = files
                .filter(path -> path.toString().endsWith(".java"))
                .flatMap(path -> violations(path).stream())
                .toList();

            assertTrue(
                violations.isEmpty(),
                () -> "Forbidden partdb-server dependency:\n" + String.join("\n", violations)
            );
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

            return violations;
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    private static Path sourceRoot() {
        Path cwd = Path.of(System.getProperty("user.dir"));
        Path moduleLocal = cwd.resolve("src/main/java/io/partdb/server");
        if (Files.isDirectory(moduleLocal)) {
            return moduleLocal;
        }
        return cwd.resolve("partdb-server/src/main/java/io/partdb/server");
    }
}
