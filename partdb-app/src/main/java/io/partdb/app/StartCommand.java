package io.partdb.app;

import io.partdb.transport.grpc.PartDbServer;
import io.partdb.transport.grpc.PartDbServerConfig;

import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

record StartCommand(
    String nodeId,
    Map<String, String> raftPeerAddresses,
    Path dataDir,
    int raftPort,
    int grpcPort,
    int adminPort
) implements AppCommand {
    private static final int DEFAULT_RAFT_PORT = 8100;
    private static final int DEFAULT_GRPC_PORT = 8101;
    private static final int DEFAULT_ADMIN_PORT = 8102;

    private static final String USAGE = """
        Usage: partdb start [options]

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
          partdb start --node-id node1 --data-dir ./data

          partdb start --node-id node1 \\
                       --raft-peer node1=192.168.1.1:8100 \\
                       --raft-peer node2=192.168.1.2:8100 \\
                       --raft-peer node3=192.168.1.3:8100 \\
                       --data-dir ./data/node1
        """;

    StartCommand {
        nodeId = CliParsing.requireNonBlank(nodeId, "nodeId");
        raftPeerAddresses = Collections.unmodifiableMap(new LinkedHashMap<>(
            Objects.requireNonNull(raftPeerAddresses, "raftPeerAddresses must not be null")
        ));
        dataDir = Objects.requireNonNull(dataDir, "dataDir must not be null");
        validatePort(raftPort, "raftPort");
        validatePort(grpcPort, "grpcPort");
        validatePort(adminPort, "adminPort");
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
            return new StartCommand(nodeId, raftPeerAddresses, dataDir, raftPort, grpcPort, adminPort);
        } catch (IllegalArgumentException e) {
            return new ErrorCommand(e.getMessage(), USAGE);
        }
    }

    @Override
    public int execute(CliRuntime runtime) {
        PartDbServerConfig config;
        try {
            config = PartDbServerConfig.create(nodeId, raftPeerAddresses, dataDir, raftPort, grpcPort, adminPort);
        } catch (IllegalArgumentException e) {
            return runtime.error(e.getMessage());
        }

        CountDownLatch shutdownLatch = new CountDownLatch(1);
        Thread shutdownHook = Thread.ofPlatform()
            .name("shutdown-hook")
            .unstarted(() -> {
                runtime.out().println("Received shutdown signal");
                shutdownLatch.countDown();
            });

        try (var server = new PartDbServer(config)) {
            Runtime.getRuntime().addShutdownHook(shutdownHook);
            try {
                server.start();
                runtime.out().println("PartDB node '" + nodeId + "' started");
                runtime.out().println("  Raft port: " + raftPort);
                runtime.out().println("  gRPC port: " + grpcPort);
                runtime.out().println("  Admin port: " + adminPort);
                runtime.out().println("  Data dir:  " + dataDir.toAbsolutePath());
                if (raftPeerAddresses.isEmpty()) {
                    runtime.out().println("  Mode:      single-node");
                } else {
                    runtime.out().println("  Raft peers: " + raftPeerAddresses.size());
                }
                runtime.out().println();
                runtime.out().println("Press Ctrl+C to stop");

                shutdownLatch.await();
            } finally {
                try {
                    Runtime.getRuntime().removeShutdownHook(shutdownHook);
                } catch (IllegalStateException ignored) {
                    // JVM shutdown is already in progress.
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            runtime.err().println("Failed to start server: " + e.getMessage());
            e.printStackTrace(runtime.err());
            return 1;
        }

        runtime.out().println("Server stopped");
        return 0;
    }

    private static void validatePort(int value, String name) {
        if (value <= 0 || value > 65535) {
            throw new IllegalArgumentException(name + " must be between 1 and 65535");
        }
    }
}
