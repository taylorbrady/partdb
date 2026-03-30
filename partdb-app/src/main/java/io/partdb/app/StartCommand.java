package io.partdb.app;

import io.partdb.transport.grpc.PartDbServer;
import io.partdb.transport.grpc.PartDbServerConfig;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

final class StartCommand {
    private static final int DEFAULT_RAFT_PORT = 8100;
    private static final int DEFAULT_GRPC_PORT = 8101;

    static int run(String[] args, PrintStream out, PrintStream err) {
        Options options;
        try {
            options = parseArgs(args);
        } catch (IllegalArgumentException e) {
            err.println("Error: " + e.getMessage());
            err.println();
            printUsage(err);
            return 1;
        }

        if (options.help) {
            printUsage(out);
            return 0;
        }

        String validationError = options.validate();
        if (validationError != null) {
            err.println("Error: " + validationError);
            err.println();
            printUsage(err);
            return 1;
        }

        PartDbServerConfig config;
        try {
            config = PartDbServerConfig.create(
                options.nodeId,
                options.raftPeerAddresses,
                options.dataDir,
                options.raftPort,
                options.grpcPort
            );
        } catch (IllegalArgumentException e) {
            err.println("Error: " + e.getMessage());
            return 1;
        }

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        try (var server = new PartDbServer(config)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                out.println("Received shutdown signal");
                shutdownLatch.countDown();
            }, "shutdown-hook"));

            server.start();
            out.println("PartDB node '" + options.nodeId + "' started");
            out.println("  Raft port: " + options.raftPort);
            out.println("  gRPC port: " + options.grpcPort);
            out.println("  Data dir:  " + options.dataDir.toAbsolutePath());
            if (options.raftPeerAddresses.isEmpty()) {
                out.println("  Mode:      single-node");
            } else {
                out.println("  Raft peers: " + options.raftPeerAddresses.size());
            }
            out.println();
            out.println("Press Ctrl+C to stop");

            shutdownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            err.println("Failed to start server: " + e.getMessage());
            e.printStackTrace(err);
            return 1;
        }

        out.println("Server stopped");
        return 0;
    }

    private static Options parseArgs(String[] args) {
        Options options = new Options();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            switch (arg) {
                case "--help", "-h" -> options.help = true;
                case "--node-id", "-n" -> options.nodeId = CliSupport.requireValue(args, ++i, "--node-id");
                case "--raft-peer", "-r" -> {
                    String peerSpec = CliSupport.requireValue(args, ++i, "--raft-peer");
                    parseRaftPeer(peerSpec, options.raftPeerAddresses);
                }
                case "--data-dir", "-d" -> options.dataDir = Path.of(CliSupport.requireValue(args, ++i, "--data-dir"));
                case "--raft-port" -> options.raftPort = CliSupport.requireIntValue(args, ++i, "--raft-port");
                case "--grpc-port" -> options.grpcPort = CliSupport.requireIntValue(args, ++i, "--grpc-port");
                default -> throw new IllegalArgumentException("Unknown option: " + arg);
            }
        }

        return options;
    }

    private static void parseRaftPeer(String peerSpec, Map<String, String> raftPeerAddresses) {
        CliSupport.NodeEndpoint peer = CliSupport.parseNodeEndpointSpec(peerSpec, "--raft-peer");
        raftPeerAddresses.put(peer.nodeId(), peer.endpoint());
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb start [options]");
        out.println();
        out.println("Start a PartDB server node.");
        out.println();
        out.println("Options:");
        out.println("  -n, --node-id <id>              Node identifier (required)");
        out.println("  -r, --raft-peer <id=endpoint>   Raft peer endpoint (repeatable, omit for single-node)");
        out.println("  -d, --data-dir <path>           Directory for data storage (required)");
        out.println("      --raft-port <port>          Port for Raft communication (default: " + DEFAULT_RAFT_PORT + ")");
        out.println("      --grpc-port <port>          Port for gRPC API traffic (default: " + DEFAULT_GRPC_PORT + ")");
        out.println("  -h, --help                      Show this help message");
        out.println();
        out.println("Examples:");
        out.println();
        out.println("  Single-node cluster:");
        out.println("    partdb start --node-id node1 --data-dir ./data");
        out.println();
        out.println("  Three-node cluster (run on node1):");
        out.println("    partdb start --node-id node1 \\");
        out.println("                 --raft-peer node1=192.168.1.1:8100 \\");
        out.println("                 --raft-peer node2=192.168.1.2:8100 \\");
        out.println("                 --raft-peer node3=192.168.1.3:8100 \\");
        out.println("                 --data-dir ./data/node1");
    }

    private static final class Options {
        boolean help;
        String nodeId;
        Map<String, String> raftPeerAddresses = new HashMap<>();
        Path dataDir;
        int raftPort = DEFAULT_RAFT_PORT;
        int grpcPort = DEFAULT_GRPC_PORT;

        String validate() {
            if (nodeId == null || nodeId.isBlank()) {
                return "--node-id is required";
            }
            if (dataDir == null) {
                return "--data-dir is required";
            }
            return null;
        }
    }
}
