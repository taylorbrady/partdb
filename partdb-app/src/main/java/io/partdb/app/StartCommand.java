package io.partdb.app;

import io.partdb.server.PartDbServer;
import io.partdb.server.PartDbServerConfig;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

record StartCommand(PartDbServerConfig config, Path dataDir) implements AppCommand {
    private static final int DEFAULT_RAFT_PORT = 8100;
    private static final int DEFAULT_GRPC_PORT = 8101;
    private static final int DEFAULT_ADMIN_PORT = 8102;

    private static final String USAGE = """
        Usage: partdb server start [options]

        Start a PartDB server node.

        Options:
          -n, --node-id <id>              Node identifier (required)
          -r, --raft-peer <id=endpoint>   Raft peer endpoint (repeatable, omit for single-node)
          -d, --data-dir <path>           Directory for data storage (required)
              --raft-port <port>          Port for Raft communication (default: 8100)
              --grpc-port <port>          Port for gRPC API traffic (default: 8101)
              --admin-port <port>         Port for admin health checks (default: 8102)
          -h, --help                      Show this help message

        Examples:
          partdb server start --node-id node1 --data-dir ./data

          partdb server start --node-id node1 \\
                       --raft-peer node1=192.168.1.1:8100 \\
                       --raft-peer node2=192.168.1.2:8100 \\
                       --raft-peer node3=192.168.1.3:8100 \\
                       --data-dir ./data/node1
        """;

    StartCommand {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(dataDir, "dataDir must not be null");
    }

    static AppCommand parse(Args args) {
        String nodeId = null;
        Path dataDir = null;
        Map<String, String> raftPeerAddresses = new LinkedHashMap<>();
        int raftPort = DEFAULT_RAFT_PORT;
        int grpcPort = DEFAULT_GRPC_PORT;
        int adminPort = DEFAULT_ADMIN_PORT;

        while (args.hasNext()) {
            String arg = args.next();
            try {
                switch (arg) {
                    case "--help", "-h" -> {
                        return new HelpCommand(USAGE);
                    }
                    case "--node-id", "-n" -> nodeId = args.requireValue("--node-id");
                    case "--raft-peer", "-r" -> {
                        var peer = CliParsing.parseNodeEndpointSpec(args.requireValue("--raft-peer"), "--raft-peer");
                        raftPeerAddresses.put(peer.nodeId(), peer.endpoint());
                    }
                    case "--data-dir", "-d" -> dataDir = Path.of(args.requireValue("--data-dir"));
                    case "--raft-port" -> raftPort = args.requireIntValue("--raft-port");
                    case "--grpc-port" -> grpcPort = args.requireIntValue("--grpc-port");
                    case "--admin-port" -> adminPort = args.requireIntValue("--admin-port");
                    default -> {
                        return new ErrorCommand("unknown option: " + arg, USAGE);
                    }
                }
            } catch (IllegalArgumentException e) {
                return new ErrorCommand(e.getMessage(), USAGE);
            }
        }

        if (nodeId == null || nodeId.isBlank()) {
            return new ErrorCommand("--node-id is required", USAGE);
        }
        if (dataDir == null) {
            return new ErrorCommand("--data-dir is required", USAGE);
        }

        try {
            var config = PartDbServerConfig.create(nodeId, raftPeerAddresses, dataDir, raftPort, grpcPort, adminPort);
            return new StartCommand(config, dataDir);
        } catch (IllegalArgumentException e) {
            return new ErrorCommand(e.getMessage(), USAGE);
        }
    }

    @Override
    public int execute(CliRuntime runtime) {
        var shutdownRequested = new CountDownLatch(1);
        var stopped = new CountDownLatch(1);
        Thread shutdownHook = Thread.ofPlatform()
            .name("shutdown-hook")
            .unstarted(() -> {
                runtime.out().println("Received shutdown signal");
                shutdownRequested.countDown();
                awaitUninterruptibly(stopped);
            });

        Runtime.getRuntime().addShutdownHook(shutdownHook);
        try {
            return runUntilShutdown(runtime, shutdownRequested);
        } finally {
            stopped.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException ignored) {
            }
        }
    }

    private int runUntilShutdown(CliRuntime runtime, CountDownLatch shutdownRequested) {
        try (var server = new PartDbServer(config)) {
            server.start();
            runtime.out().println("PartDB node '" + config.nodeId() + "' started");
            runtime.out().println("  Raft port: " + config.raftPort());
            runtime.out().println("  gRPC port: " + config.grpcPort());
            runtime.out().println("  Admin port: " + config.adminPort());
            runtime.out().println("  Data dir:  " + dataDir.toAbsolutePath());
            if (config.raftPeerEndpoints().isEmpty()) {
                runtime.out().println("  Mode:      single-node");
            } else {
                runtime.out().println("  Raft peers: " + config.raftPeerEndpoints().size());
            }
            runtime.out().println();
            runtime.out().println("Press Ctrl+C to stop");

            shutdownRequested.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            runtime.err().println("Failed to start server: " + e.getMessage());
            e.printStackTrace(runtime.err());
            return 1;
        }

        runtime.out().println("Server stopped");
        runtime.out().flush();
        return 0;
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
