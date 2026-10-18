package io.partdb.app;

import io.partdb.server.PartDbServer;
import io.partdb.server.ServerConfig;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Objects;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

record StartCommand(ServerConfig config) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb server start [options]

        Start a PartDB server node.

        Options:
          -n, --node-id <id>             Node identifier (required)
          -d, --data-dir <path>          Directory for node data (required)
          -p, --peer <id=host:port>      Raft address of a cluster member, including this node
                                         (repeatable; omit for a single-node cluster)
              --client-port <port>       Port for client gRPC traffic (default: %d)
              --admin-port <port>        Port for health checks (default: %d)
              --cluster-id <id>          Cluster identifier (default: derived from the peers)
              --snapshot-entries <n>     Log entries between snapshots (default: %d)
              --snapshot-bytes <n>       Log bytes between snapshots (default: %d)
          -h, --help                     Show this help message

        Examples:
          partdb server start --node-id node1 --data-dir ./data

          partdb server start --node-id node1 --data-dir ./data/node1 \\
                              --peer node1=10.0.0.1:8100 \\
                              --peer node2=10.0.0.2:8100 \\
                              --peer node3=10.0.0.3:8100
        """.formatted(
        ServerConfig.DEFAULT_CLIENT_PORT, ServerConfig.DEFAULT_ADMIN_PORT, ServerConfig.DEFAULT_SNAPSHOTS.logEntries(), ServerConfig.DEFAULT_SNAPSHOTS.logBytes());

    StartCommand {
        Objects.requireNonNull(config, "config must not be null");
    }

    static AppCommand parse(Args args) {
        String nodeId = null;
        Path dataDir = null;
        List<Consumer<ServerConfig.Builder>> settings = new ArrayList<>();
        try {
            while (args.hasNext()) {
                String arg = args.next();
                switch (arg) {
                    case "--help", "-h" -> {
                        return new Help(USAGE);
                    }
                    case "--node-id", "-n" -> nodeId = args.value(arg);
                    case "--data-dir", "-d" -> dataDir = Path.of(args.value(arg));
                    case "--peer", "-p" -> {
                        String peer = args.value(arg);
                        int separator = peer.indexOf('=');
                        if (separator <= 0 || separator == peer.length() - 1) {
                            throw new IllegalArgumentException("--peer must look like id=host:port, got: " + peer);
                        }
                        String id = peer.substring(0, separator).trim();
                        String address = peer.substring(separator + 1).trim();
                        settings.add(builder -> builder.peer(id, address));
                    }
                    case "--client-port" -> {
                        int port = args.intValue(arg);
                        settings.add(builder -> builder.clientPort(port));
                    }
                    case "--admin-port" -> {
                        int port = args.intValue(arg);
                        settings.add(builder -> builder.adminPort(port));
                    }
                    case "--cluster-id" -> {
                        String clusterId = args.value(arg);
                        settings.add(builder -> builder.clusterId(clusterId));
                    }
                    case "--snapshot-entries" -> {
                        long entries = args.longValue(arg);
                        settings.add(builder -> builder.snapshotLogEntries(entries));
                    }
                    case "--snapshot-bytes" -> {
                        long bytes = args.longValue(arg);
                        settings.add(builder -> builder.snapshotLogBytes(bytes));
                    }
                    default -> throw new IllegalArgumentException("unknown option: " + arg);
                }
            }
            if (nodeId == null) {
                throw new IllegalArgumentException("--node-id is required");
            }
            if (dataDir == null) {
                throw new IllegalArgumentException("--data-dir is required");
            }
            var builder = ServerConfig.builder(nodeId, dataDir);
            settings.forEach(setting -> setting.accept(builder));
            return new StartCommand(builder.build());
        } catch (IllegalArgumentException e) {
            return new UsageError(e.getMessage(), USAGE);
        }
    }

    @Override
    public ExitCode execute(PrintStream out, PrintStream err) {
        var shutdownRequested = new CountDownLatch(1);
        var stopped = new CountDownLatch(1);
        Thread hook = Thread.ofPlatform().name("shutdown-hook").unstarted(() -> {
            out.println("Received shutdown signal");
            shutdownRequested.countDown();
            awaitUninterruptibly(stopped);
        });
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            return serve(out, err, shutdownRequested);
        } finally {
            stopped.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException _) {
            }
        }
    }

    private ExitCode serve(PrintStream out, PrintStream err, CountDownLatch shutdownRequested) {
        try (var _ = PartDbServer.start(config)) {
            out.println("PartDB node '" + config.nodeId() + "' started");
            out.println("  Cluster:     " + config.clusterId());
            out.println("  Client port: " + config.clientPort());
            out.println("  Admin port:  " + config.adminPort());
            config.raftPort().ifPresentOrElse(
                port -> out.println("  Raft port:   " + port + " (" + config.peers().size() + " members)"),
                () -> out.println("  Mode:        single-node"));
            out.println("  Data dir:    " + config.dataDirectory().toAbsolutePath());
            shutdownRequested.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace(err);
            return ExitCode.FAILED;
        }
        out.println("Server stopped");
        out.flush();
        return ExitCode.OK;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }
}
