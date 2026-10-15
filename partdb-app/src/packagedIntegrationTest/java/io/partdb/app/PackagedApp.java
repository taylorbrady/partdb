package io.partdb.app;

import io.partdb.app.testing.CommandResult;
import io.partdb.app.testing.NodeSpec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

final class PackagedApp {
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(45);

    private final Path executable;

    private PackagedApp(Path executable) {
        this.executable = executable;
    }

    static PackagedApp fromInstallDir() {
        Path installDir = Path.of(System.getProperty("partdb.app.installDir"));
        boolean windows = System.getProperty("os.name").startsWith("Windows");
        return new PackagedApp(installDir.resolve("bin").resolve(windows ? "partdb-app.bat" : "partdb-app"));
    }

    PackagedNode newNode(Path rootDir, NodeSpec spec) {
        return new PackagedNode(executable, rootDir.resolve(spec.nodeId()), spec);
    }

    CommandResult run(String... args) throws IOException, InterruptedException {
        var command = new ArrayList<String>();
        command.add(executable.toString());
        Collections.addAll(command, args);

        Process process = new ProcessBuilder(command).start();
        var stdout = new ByteArrayOutputStream();
        var stderr = new ByteArrayOutputStream();
        Thread stdoutDrainer = drain(process.getInputStream(), stdout);
        Thread stderrDrainer = drain(process.getErrorStream(), stderr);

        if (!process.waitFor(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new AssertionError("Timed out waiting for packaged command: " + String.join(" ", command));
        }
        stdoutDrainer.join();
        stderrDrainer.join();

        return new CommandResult(
            process.exitValue(),
            stdout.toString(StandardCharsets.UTF_8),
            stderr.toString(StandardCharsets.UTF_8)
        );
    }

    static Thread drain(InputStream input, ByteArrayOutputStream target) {
        return Thread.ofVirtual().start(() -> {
            try (input) {
                input.transferTo(target);
            } catch (IOException ignored) {
            }
        });
    }
}
