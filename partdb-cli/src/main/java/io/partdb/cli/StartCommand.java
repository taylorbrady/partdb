package io.partdb.cli;

import io.partdb.server.PartDb;
import io.partdb.server.PartDbConfig;

import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

        PartDbConfig config = PartDbConfig.create(
            options.nodeId,
            options.peers,
            options.peerAddresses,
            options.dataDir,
            options.raftPort,
            options.kvPort
        );

        CountDownLatch shutdownLatch = new CountDownLatch(1);

        try (PartDb server = new PartDb(config)) {
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                out.println("Received shutdown signal");
                shutdownLatch.countDown();
            }, "shutdown-hook"));

            server.start();
            out.println("PartDB node '" + options.nodeId + "' started");
            out.println("  Raft port: " + options.raftPort);
            out.println("  KV port:   " + options.kvPort);
            out.println("  Data dir:  " + options.dataDir.toAbsolutePath());
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
                case "--peers", "-p" -> {
                    String peersStr = requireValue(args, ++i, "--peers");
                    options.peers = List.of(peersStr.split(","));
                }
                case "--peer-address" -> {
                    String peerAddr = requireValue(args, ++i, "--peer-address");
                    int eq = peerAddr.indexOf('=');
                    if (eq <= 0) {
                        throw new IllegalArgumentException(
                            "--peer-address must be in format nodeId=host:port, got: " + peerAddr);
                    }
                    String peerId = peerAddr.substring(0, eq);
                    String address = peerAddr.substring(eq + 1);
                    options.peerAddresses.put(peerId, address);
                }
                case "--data-dir", "-d" -> {
                    options.dataDir = Path.of(requireValue(args, ++i, "--data-dir"));
                }
                case "--raft-port" -> {
                    options.raftPort = Integer.parseInt(requireValue(args, ++i, "--raft-port"));
                }
                case "--kv-port" -> {
                    options.kvPort = Integer.parseInt(requireValue(args, ++i, "--kv-port"));
                }
                default -> throw new IllegalArgumentException("Unknown option: " + arg);
            }
        }

        return options;
    }

    private static String requireValue(String[] args, int index, String optionName) {
        if (index >= args.length) {
            throw new IllegalArgumentException(optionName + " requires a value");
        }
        return args[index];
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb start [options]");
        out.println();
        out.println("Start a PartDB server node.");
        out.println();
        out.println("Options:");
        out.println("  -n, --node-id <id>              Node identifier (required)");
        out.println("  -p, --peers <id1,id2,...>       Comma-separated list of all node IDs in the cluster (required)");
        out.println("      --peer-address <id=host:port>  Address for a peer node (repeatable, required for each peer)");
        out.println("  -d, --data-dir <path>           Directory for data storage (required)");
        out.println("      --raft-port <port>          Port for Raft communication (default: " + DEFAULT_RAFT_PORT + ")");
        out.println("      --kv-port <port>            Port for KV service (default: " + DEFAULT_KV_PORT + ")");
        out.println("  -h, --help                      Show this help message");
        out.println();
        out.println("Example:");
        out.println("  partdb start --node-id node1 \\");
        out.println("               --peers node1,node2,node3 \\");
        out.println("               --peer-address node1=localhost:8100 \\");
        out.println("               --peer-address node2=localhost:8200 \\");
        out.println("               --peer-address node3=localhost:8300 \\");
        out.println("               --data-dir ./data/node1 \\");
        out.println("               --raft-port 8100 \\");
        out.println("               --kv-port 8101");
    }

    private static final class Options {
        boolean help;
        String nodeId;
        List<String> peers = new ArrayList<>();
        Map<String, String> peerAddresses = new HashMap<>();
        Path dataDir;
        int raftPort = DEFAULT_RAFT_PORT;
        int kvPort = DEFAULT_KV_PORT;

        String validate() {
            if (nodeId == null || nodeId.isBlank()) {
                return "--node-id is required";
            }
            if (peers.isEmpty()) {
                return "--peers is required";
            }
            if (dataDir == null) {
                return "--data-dir is required";
            }
            for (String peer : peers) {
                if (!peerAddresses.containsKey(peer)) {
                    return "--peer-address required for peer: " + peer;
                }
            }
            return null;
        }
    }
}
