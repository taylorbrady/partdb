package io.partdb.app;

import io.partdb.client.ServerEndpoint;
import io.partdb.client.testing.NodeSpec;
import io.partdb.client.testing.TestClusterNode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

final class PackagedNode implements TestClusterNode {
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);
    private static final String CLEAN_STOP_MARKER = "Server stopped";

    private final Path executable;
    private final Path nodeDir;
    private final NodeSpec spec;

    private int startCount;
    private Process process;
    private Path stdoutLog;
    private Path stderrLog;

    PackagedNode(Path executable, Path nodeDir, NodeSpec spec) {
        this.executable = executable;
        this.nodeDir = nodeDir;
        this.spec = spec;
    }

    @Override
    public String nodeId() {
        return spec.nodeId();
    }

    @Override
    public ServerEndpoint endpoint() {
        return spec.grpcEndpoint();
    }

    @Override
    public boolean isRunning() {
        return process != null && process.isAlive();
    }

    @Override
    public void start() throws IOException {
        if (isRunning()) {
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
        for (var peer : spec.raftPeerAddresses().entrySet()) {
            command.add("--raft-peer");
            command.add(peer.getKey() + "=" + peer.getValue());
        }
        command.add("--data-dir");
        command.add(nodeDir.resolve("data").toString());
        command.add("--raft-port");
        command.add(Integer.toString(spec.raftPort()));
        command.add("--grpc-port");
        command.add(Integer.toString(spec.grpcPort()));
        command.add("--admin-port");
        command.add(Integer.toString(spec.adminPort()));

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
            if (!read(stdoutLog).contains(CLEAN_STOP_MARKER)) {
                throw new AssertionError(nodeId() + " did not shut down cleanly" + output());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        } finally {
            process = null;
        }
    }

    @Override
    public void assertStillRunning() {
        if (!isRunning()) {
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
