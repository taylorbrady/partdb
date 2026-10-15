package io.partdb.app;

import io.partdb.raft.runtime.ClusterId;
import io.partdb.raft.runtime.SnapshotPolicy;
import io.partdb.server.PartDbServer;
import io.partdb.server.ServerConfig;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

record StartCommand(ServerConfig config) implements AppCommand {
    private static final int DEFAULT_CLIENT_PORT = 8101;
    private static final int DEFAULT_ADMIN_PORT = 8102;

    private static final String USAGE = """
        Usage: partdb server start [options]

        Start a PartDB server node.

        Options:
          -n, --node-id <id>             Node identifier (required)
          -d, --data-dir <path>          Directory for node data (required)
          -p, --peer <id=host:port>      Raft address of a cluster member, including this node
                                         (repeatable; omit for a single-node cluster)
              --client-port <port>       Port for client gRPC traffic (default: 8101)
              --admin-port <port>        Port for health checks (default: 8102)
              --cluster-id <id>          Cluster identifier (default: derived from the peers)
              --snapshot-entries <n>     Log entries between snapshots (default: 10000)
              --snapshot-bytes <n>       Log bytes between snapshots (default: 67108864)
          -h, --help                     Show this help message

        Examples:
          partdb server start --node-id node1 --data-dir ./data

          partdb server start --node-id node1 --data-dir ./data/node1 \\
                              --peer node1=10.0.0.1:8100 \\
                              --peer node2=10.0.0.2:8100 \\
                              --peer node3=10.0.0.3:8100
        """;

    StartCommand {
        Objects.requireNonNull(config, "config must not be null");
    }

    static AppCommand parse(Args args) {
        String nodeId = null;
        Path dataDir = null;
        var peers = new LinkedHashMap<String, String>();
        int clientPort = DEFAULT_CLIENT_PORT;
        int adminPort = DEFAULT_ADMIN_PORT;
        Optional<ClusterId> clusterId = Optional.empty();
        long snapshotEntries = SnapshotPolicy.DEFAULT.logEntries();
        long snapshotBytes = SnapshotPolicy.DEFAULT.logBytes();
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
                        peers.put(peer.substring(0, separator).trim(), peer.substring(separator + 1).trim());
                    }
                    case "--client-port" -> clientPort = args.intValue(arg);
                    case "--admin-port" -> adminPort = args.intValue(arg);
                    case "--cluster-id" -> clusterId = Optional.of(ClusterId.of(args.value(arg)));
                    case "--snapshot-entries" -> snapshotEntries = args.longValue(arg);
                    case "--snapshot-bytes" -> snapshotBytes = args.longValue(arg);
                    default -> throw new IllegalArgumentException("unknown option: " + arg);
                }
            }
            if (nodeId == null) {
                throw new IllegalArgumentException("--node-id is required");
            }
            if (dataDir == null) {
                throw new IllegalArgumentException("--data-dir is required");
            }
            return new StartCommand(ServerConfig.create(
                nodeId, peers, dataDir, clientPort, adminPort, clusterId, new SnapshotPolicy(snapshotEntries, snapshotBytes)));
        } catch (IllegalArgumentException e) {
            return new UsageError(e.getMessage(), USAGE);
        }
    }

    @Override
    public int execute(PrintStream out, PrintStream err) {
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

    private int serve(PrintStream out, PrintStream err, CountDownLatch shutdownRequested) {
        try (var server = new PartDbServer(config)) {
            server.start();
            out.println("PartDB node '" + config.nodeId() + "' started");
            out.println("  Cluster:     " + config.node().raft().clusterId());
            out.println("  Client port: " + config.clientPort());
            out.println("  Admin port:  " + config.adminPort());
            config.raftPort().ifPresentOrElse(
                port -> out.println("  Raft port:   " + port + " (" + config.peers().size() + " members)"),
                () -> out.println("  Mode:        single-node"));
            out.println("  Data dir:    " + config.node().dataDirectory().toAbsolutePath());
            shutdownRequested.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace(err);
            return FAILED;
        }
        out.println("Server stopped");
        out.flush();
        return OK;
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
