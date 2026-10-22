package io.partdb.app;

import io.partdb.server.PartDbServer;
import io.partdb.server.ServerConfig;

import java.io.PrintStream;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static java.util.Map.entry;

record StartCommand(ServerConfig config, OutputFormat logFormat, Level logLevel) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb server start [options]

        Start a PartDB server node.

        Options:
          -n, --node-id <id>             Node identifier (required)
          -d, --data-dir <path>          Directory for node data (required)
          -p, --peer <id=host:port>      Raft address of a cluster member, including this node
                                         (repeatable; omit for a single-node cluster)
              --client-port <port>       Port for client gRPC traffic (default: %d)
              --admin-port <port>        Port for health checks and metrics (default: %d)
              --cluster-id <id>          Cluster identifier for a new node (default: derived from the peers)
              --log-retention-entries <n>
                                         Durable log entries kept for lagging peers (default: %d)
              --log-retention-bytes <n>  Durable log bytes kept for lagging peers (default: %d)
              --log-format <format>      Log format: text or json (default: text)
              --log-level <level>        Log level: error, warn, info, debug, or trace (default: info)
          -h, --help                     Show this help message

        Examples:
          partdb server start --node-id node1 --data-dir ./data

          partdb server start --node-id node1 --data-dir ./data/node1 \\
                              --peer node1=10.0.0.1:8100 \\
                              --peer node2=10.0.0.2:8100 \\
                              --peer node3=10.0.0.3:8100
        """.formatted(
        ServerConfig.DEFAULT_CLIENT_PORT, ServerConfig.DEFAULT_ADMIN_PORT, ServerConfig.DEFAULT_RETENTION.entries(), ServerConfig.DEFAULT_RETENTION.bytes());

    StartCommand {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(logFormat, "logFormat must not be null");
        Objects.requireNonNull(logLevel, "logLevel must not be null");
    }

    static AppCommand parse(Args args) {
        String nodeId = null;
        Path dataDir = null;
        OutputFormat logFormat = OutputFormat.TEXT;
        Level logLevel = Level.INFO;
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
                    case "--log-retention-entries" -> {
                        long entries = args.longValue(arg);
                        settings.add(builder -> builder.logRetentionEntries(entries));
                    }
                    case "--log-retention-bytes" -> {
                        long bytes = args.longValue(arg);
                        settings.add(builder -> builder.logRetentionBytes(bytes));
                    }
                    case "--log-format" -> logFormat = switch (args.value(arg)) {
                        case "text" -> OutputFormat.TEXT;
                        case "json" -> OutputFormat.JSON;
                        case String other -> throw new IllegalArgumentException("--log-format must be text or json, got: " + other);
                    };
                    case "--log-level" -> logLevel = switch (args.value(arg)) {
                        case "error" -> Level.ERROR;
                        case "warn" -> Level.WARNING;
                        case "info" -> Level.INFO;
                        case "debug" -> Level.DEBUG;
                        case "trace" -> Level.TRACE;
                        case String other -> throw new IllegalArgumentException("--log-level must be error, warn, info, debug, or trace, got: " + other);
                    };
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
            return new StartCommand(builder.build(), logFormat, logLevel);
        } catch (IllegalArgumentException e) {
            return new UsageError(e.getMessage(), USAGE);
        }
    }

    @Override
    public ExitCode execute(PrintStream out, PrintStream err) {
        Logging.configure(logFormat, logLevel, config.nodeId().value());
        Logger log = System.getLogger(StartCommand.class.getName());
        var shutdownRequested = new CountDownLatch(1);
        var stopped = new CountDownLatch(1);
        Thread hook = Thread.ofPlatform().name("shutdown-hook").unstarted(() -> {
            shutdownRequested.countDown();
            awaitUninterruptibly(stopped);
        });
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            return serve(log, shutdownRequested);
        } finally {
            stopped.countDown();
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException _) {
            }
        }
    }

    private ExitCode serve(Logger log, CountDownLatch shutdownRequested) {
        log.log(Level.INFO, "Starting PartDB",
            entry("version", PartDbApp.VERSION),
            entry("java", Runtime.version()),
            entry("dataDir", config.dataDirectory().toAbsolutePath()));
        try (var _ = PartDbServer.start(config)) {
            shutdownRequested.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.log(Level.ERROR, "PartDB server failed", e);
            return ExitCode.FAILED;
        }
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
