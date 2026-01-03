package io.partdb.cli;

import io.partdb.server.PartDbServerConfig;
import io.partdb.server.PartDbServer;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

final class StartCommand {
    private static final int DEFAULT_RAFT_PORT = 8100;
    private static final int DEFAULT_KV_PORT = 8101;

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
                options.peerAddresses,
                options.dataDir,
                options.raftPort,
                options.kvPort
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
            out.println("  KV port:   " + options.kvPort);
            out.println("  Data dir:  " + options.dataDir.toAbsolutePath());
            if (options.peerAddresses.isEmpty()) {
                out.println("  Mode:      single-node");
            } else {
                out.println("  Peers:     " + options.peerAddresses.size());
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
                case "--node-id", "-n" -> {
                    options.nodeId = requireValue(args, ++i, "--node-id");
                }
                case "--peer", "-p" -> {
                    String peerSpec = requireValue(args, ++i, "--peer");
                    parsePeer(peerSpec, options.peerAddresses);
                }
                case "--data-dir", "-d" -> {
                    options.dataDir = Path.of(requireValue(args, ++i, "--data-dir"));
                }
                case "--raft-port" -> {
                    options.raftPort = requireIntValue(args, ++i, "--raft-port");
                }
                case "--kv-port" -> {
                    options.kvPort = requireIntValue(args, ++i, "--kv-port");
                }
                default -> throw new IllegalArgumentException("Unknown option: " + arg);
            }
        }

        return options;
    }

    private static void parsePeer(String peerSpec, Map<String, String> peerAddresses) {
        String[] parts = peerSpec.split("=", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                "Invalid peer format: '" + peerSpec + "'. Expected: nodeId=host:port"
            );
        }
        String nodeId = parts[0].trim();
        String address = parts[1].trim();
        if (nodeId.isEmpty() || address.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid peer format: '" + peerSpec + "'. Expected: nodeId=host:port"
            );
        }
        if (!address.contains(":")) {
            throw new IllegalArgumentException(
                "Invalid peer address: '" + address + "'. Expected: host:port"
            );
        }
        peerAddresses.put(nodeId, address);
    }

    private static String requireValue(String[] args, int index, String optionName) {
        if (index >= args.length) {
            throw new IllegalArgumentException(optionName + " requires a value");
        }
        return args[index];
    }

    private static int requireIntValue(String[] args, int index, String optionName) {
        String value = requireValue(args, index, optionName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(optionName + " must be a valid integer, got: " + value);
        }
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb start [options]");
        out.println();
        out.println("Start a PartDB server node.");
        out.println();
        out.println("Options:");
        out.println("  -n, --node-id <id>              Node identifier (required)");
        out.println("  -p, --peer <id=host:port>       Peer node (repeatable, omit for single-node)");
        out.println("  -d, --data-dir <path>           Directory for data storage (required)");
        out.println("      --raft-port <port>          Port for Raft communication (default: " + DEFAULT_RAFT_PORT + ")");
        out.println("      --kv-port <port>            Port for KV service (default: " + DEFAULT_KV_PORT + ")");
        out.println("  -h, --help                      Show this help message");
        out.println();
        out.println("Examples:");
        out.println();
        out.println("  Single-node cluster:");
        out.println("    partdb start --node-id node1 --data-dir ./data");
        out.println();
        out.println("  Three-node cluster (run on node1):");
        out.println("    partdb start --node-id node1 \\");
        out.println("                 --peer node1=192.168.1.1:8100 \\");
        out.println("                 --peer node2=192.168.1.2:8100 \\");
        out.println("                 --peer node3=192.168.1.3:8100 \\");
        out.println("                 --data-dir ./data/node1");
    }

    private static final class Options {
        boolean help;
        String nodeId;
        Map<String, String> peerAddresses = new HashMap<>();
        Path dataDir;
        int raftPort = DEFAULT_RAFT_PORT;
        int kvPort = DEFAULT_KV_PORT;

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
