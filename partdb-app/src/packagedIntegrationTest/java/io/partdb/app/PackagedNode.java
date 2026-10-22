package io.partdb.app;

import io.partdb.app.testing.NodeSpec;
import io.partdb.app.testing.TestClusterNode;
import io.partdb.client.ServerEndpoint;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class PackagedNode implements TestClusterNode {
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final String CLEAN_STOP_MARKER = "PartDB server stopped";

    private final Path executable;
    private final Path nodeDir;
    private final NodeSpec spec;
    private final List<String> options;

    private int startCount;
    private Process process;
    private Path stdoutLog;
    private Path stderrLog;

    PackagedNode(Path executable, Path nodeDir, NodeSpec spec, List<String> options) {
        this.executable = executable;
        this.nodeDir = nodeDir;
        this.spec = spec;
        this.options = List.copyOf(options);
    }

    @Override
    public String nodeId() {
        return spec.nodeId();
    }

    @Override
    public ServerEndpoint endpoint() {
        return spec.endpoint();
    }

    @Override
    public boolean isStarted() {
        return process != null;
    }

    @Override
    public void start() throws IOException {
        if (process != null) {
            return;
        }

        Files.createDirectories(nodeDir);
        startCount++;
        stdoutLog = nodeDir.resolve("stdout-" + startCount + ".log");
        stderrLog = nodeDir.resolve("stderr-" + startCount + ".log");

        var command = new ArrayList<String>();
        command.add(executable.toString());
        command.add("server");
        command.add("start");
        command.add("--node-id");
        command.add(spec.nodeId());
        for (var peer : spec.peers().entrySet()) {
            command.add("--peer");
            command.add(peer.getKey() + "=" + peer.getValue());
        }
        command.add("--data-dir");
        command.add(nodeDir.resolve("data").toString());
        command.add("--client-port");
        command.add(Integer.toString(spec.clientPort()));
        command.add("--admin-port");
        command.add(Integer.toString(spec.adminPort()));
        command.add("--log-format");
        command.add("json");
        command.addAll(options);

        process = new ProcessBuilder(command)
            .redirectOutput(stdoutLog.toFile())
            .redirectError(stderrLog.toFile())
            .start();
    }

    @Override
    public void stop() {
        if (process == null) {
            return;
        }
        try {
            process.destroy();
            if (!process.waitFor(STOP_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError(nodeId() + " did not exit within " + STOP_TIMEOUT + " of SIGTERM" + output());
            }
            String stdout = read(stdoutLog);
            if (!stdout.contains(CLEAN_STOP_MARKER)) {
                throw new AssertionError(nodeId() + " did not shut down cleanly" + output());
            }
            if (!stdout.lines().allMatch(line -> line.startsWith("{\"time\":") && line.endsWith("}"))) {
                throw new AssertionError(nodeId() + " wrote a stdout line that is not a JSON log record" + output());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        } finally {
            process = null;
        }
    }

    void kill() {
        if (process == null) {
            return;
        }
        try {
            process.destroyForcibly();
            if (!process.waitFor(STOP_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                throw new AssertionError(nodeId() + " did not exit within " + STOP_TIMEOUT + " of SIGKILL");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            process = null;
        }
    }

    String log() {
        return read(stdoutLog);
    }

    @Override
    public void checkAlive() {
        if (process != null && !process.isAlive()) {
            throw new AssertionError(nodeId() + " exited unexpectedly" + output());
        }
    }

    private String output() {
        return "\nstdout (" + stdoutLog + "):\n" + read(stdoutLog) + "\nstderr (" + stderrLog + "):\n" + read(stderrLog);
    }

    private static String read(Path log) {
        try {
            return Files.readString(log, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
