package io.partdb.app;

import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterClientConfig;
import io.partdb.client.ClusterNodeRole;
import io.partdb.client.ClusterStatus;
import io.partdb.client.ServerEndpoint;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

final class PackagedClusterHarness implements AutoCloseable {
    private static final Duration COMMAND_TIMEOUT = Duration.ofSeconds(45);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(100);

    private final Path executable;
    private final LinkedHashMap<String, NodeProcess> nodes;

    private PackagedClusterHarness(Path executable, LinkedHashMap<String, NodeProcess> nodes) {
        this.executable = executable;
        this.nodes = nodes;
    }

    static PackagedClusterHarness create(Path executable, Path rootDir, int nodeCount) throws IOException {
        if (nodeCount < 3) {
            throw new IllegalArgumentException("nodeCount must be at least 3");
        }

        record NodeSpec(String nodeId, int raftPort, int grpcPort, int adminPort) {}

        var specs = new ArrayList<NodeSpec>(nodeCount);
        for (int i = 1; i <= nodeCount; i++) {
            specs.add(new NodeSpec("node" + i, freePort(), freePort(), freePort()));
        }

        var raftPeerAddresses = new LinkedHashMap<String, String>();
        for (NodeSpec spec : specs) {
            raftPeerAddresses.put(spec.nodeId(), "localhost:" + spec.raftPort());
        }

        var nodes = new LinkedHashMap<String, NodeProcess>();
        for (NodeSpec spec : specs) {
            nodes.put(spec.nodeId(), new NodeProcess(
                executable,
                spec.nodeId(),
                rootDir.resolve(spec.nodeId()),
                spec.raftPort(),
                spec.grpcPort(),
                spec.adminPort(),
                raftPeerAddresses
            ));
        }

        return new PackagedClusterHarness(executable, nodes);
    }

    void startAll() throws IOException {
        for (NodeProcess node : nodes.values()) {
            node.start();
        }
    }

    void stopNode(String nodeId) {
        node(nodeId).stop();
    }

    void startNode(String nodeId) throws IOException {
        node(nodeId).start();
    }

    NodeProcess awaitStableLeader(Duration timeout) throws Exception {
        return awaitStableLeader(timeout, null);
    }

    NodeProcess awaitStableLeaderExcluding(String excludedNodeId, Duration timeout) throws Exception {
        return awaitStableLeader(timeout, excludedNodeId);
    }

    String grpcAddress(String nodeId) {
        return node(nodeId).endpoint().toString();
    }

    CommandResult runCommand(String... args) throws Exception {
        var command = new ArrayList<String>();
        command.add(executable.toString());
        java.util.Collections.addAll(command, args);

        Process process = new ProcessBuilder(command).start();
        byte[] stdout;
        byte[] stderr;
        try (InputStream out = process.getInputStream(); InputStream err = process.getErrorStream()) {
            if (!process.waitFor(COMMAND_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("Timed out waiting for packaged command: " + String.join(" ", command));
            }
            stdout = out.readAllBytes();
            stderr = err.readAllBytes();
        }

        return new CommandResult(
            process.exitValue(),
            new String(stdout, StandardCharsets.UTF_8),
            new String(stderr, StandardCharsets.UTF_8)
        );
    }

    @Override
    public void close() {
        var reverse = new ArrayList<>(nodes.values());
        java.util.Collections.reverse(reverse);
        for (NodeProcess node : reverse) {
            node.stop();
        }
    }

    private NodeProcess awaitStableLeader(Duration timeout, String excludedNodeId) throws Exception {
        long deadlineNanos = System.nanoTime() + timeout.toNanos();
        Throwable lastFailure = null;

        while (System.nanoTime() < deadlineNanos) {
            var runningNodes = nodes.values().stream().filter(NodeProcess::isRunning).toList();
            if (runningNodes.isEmpty()) {
                throw new IllegalStateException("No running packaged nodes");
            }

            var statuses = new LinkedHashMap<String, ClusterStatus>();
            boolean allAvailable = true;
            for (NodeProcess node : runningNodes) {
                try (var client = new ClusterClient(ClusterClientConfig.defaultConfig(node.endpoint()))) {
                    statuses.put(node.nodeId(), client.status().get());
                } catch (Exception e) {
                    lastFailure = e;
                    allAvailable = false;
                    break;
                }
            }

            if (allAvailable) {
                var leaders = statuses.entrySet().stream()
                    .filter(entry -> entry.getValue().role() == ClusterNodeRole.LEADER && entry.getValue().running())
                    .map(Map.Entry::getKey)
                    .toList();

                if (leaders.size() == 1) {
                    String leaderId = leaders.get(0);
                    if ((excludedNodeId == null || !excludedNodeId.equals(leaderId))
                        && statuses.values().stream().allMatch(status ->
                            status.running() && effectiveLeaderId(status).map(leaderId::equals).orElse(false))) {
                        return node(leaderId);
                    }
                }
            }

            for (NodeProcess node : runningNodes) {
                node.assertStillRunning();
            }

            Thread.sleep(POLL_INTERVAL);
        }

        throw new AssertionError("Timed out waiting for stable packaged leader", lastFailure);
    }

    private NodeProcess node(String nodeId) {
        NodeProcess node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Unknown node: " + nodeId);
        }
        return node;
    }

    private static Optional<String> effectiveLeaderId(ClusterStatus status) {
        if (status.leaderId().isPresent()) {
            return status.leaderId();
        }
        if (status.role() == ClusterNodeRole.LEADER) {
            return Optional.of(status.nodeId());
        }
        return Optional.empty();
    }

    private static int freePort() throws IOException {
        try (var socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    record CommandResult(int exitCode, String stdout, String stderr) {}

    static final class NodeProcess {
        private final Path executable;
        private final String nodeId;
        private final Path dataDir;
        private final int raftPort;
        private final int grpcPort;
        private final int adminPort;
        private final LinkedHashMap<String, String> raftPeerAddresses;
        private final ByteArrayOutputStream stdoutBuffer = new ByteArrayOutputStream();
        private final ByteArrayOutputStream stderrBuffer = new ByteArrayOutputStream();

        private Process process;
        private Thread stdoutThread;
        private Thread stderrThread;

        NodeProcess(
            Path executable,
            String nodeId,
            Path dataDir,
            int raftPort,
            int grpcPort,
            int adminPort,
            LinkedHashMap<String, String> raftPeerAddresses
        ) {
            this.executable = executable;
            this.nodeId = nodeId;
            this.dataDir = dataDir;
            this.raftPort = raftPort;
            this.grpcPort = grpcPort;
            this.adminPort = adminPort;
            this.raftPeerAddresses = new LinkedHashMap<>(raftPeerAddresses);
        }

        String nodeId() {
            return nodeId;
        }

        ServerEndpoint endpoint() {
            return new ServerEndpoint("localhost", grpcPort);
        }

        boolean isRunning() {
            return process != null && process.isAlive();
        }

        void start() throws IOException {
            if (isRunning()) {
                return;
            }

            stdoutBuffer.reset();
            stderrBuffer.reset();

            var command = new ArrayList<String>();
            command.add(executable.toString());
            command.add("server");
            command.add("start");
            command.add("--node-id");
            command.add(nodeId);
            for (var entry : raftPeerAddresses.entrySet()) {
                command.add("--raft-peer");
                command.add(entry.getKey() + "=" + entry.getValue());
            }
            command.add("--data-dir");
            command.add(dataDir.toString());
            command.add("--raft-port");
            command.add(Integer.toString(raftPort));
            command.add("--grpc-port");
            command.add(Integer.toString(grpcPort));
            command.add("--admin-port");
            command.add(Integer.toString(adminPort));

            process = new ProcessBuilder(command).start();
            stdoutThread = startDrainer(process.getInputStream(), stdoutBuffer);
            stderrThread = startDrainer(process.getErrorStream(), stderrBuffer);
        }

        void stop() {
            if (process == null) {
                return;
            }
            process.destroy();
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    process.destroyForcibly();
                    process.waitFor(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                process.destroyForcibly();
            } finally {
                process = null;
                stdoutThread = null;
                stderrThread = null;
            }
        }

        void assertStillRunning() {
            if (!isRunning()) {
                throw new AssertionError(
                    "Packaged node " + nodeId + " exited unexpectedly.\nstdout:\n" + stdout()
                        + "\nstderr:\n" + stderr()
                );
            }
        }

        String stdout() {
            return stdoutBuffer.toString(StandardCharsets.UTF_8);
        }

        String stderr() {
            return stderrBuffer.toString(StandardCharsets.UTF_8);
        }

        private static Thread startDrainer(InputStream input, ByteArrayOutputStream target) {
            Objects.requireNonNull(input, "input must not be null");
            Objects.requireNonNull(target, "target must not be null");
            Thread thread = Thread.ofVirtual().start(() -> {
                try (input; PrintStream out = new PrintStream(target, true, StandardCharsets.UTF_8)) {
                    input.transferTo(out);
                } catch (IOException ignored) {
                }
            });
            return thread;
        }
    }
}
