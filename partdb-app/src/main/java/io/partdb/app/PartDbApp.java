package io.partdb.app;

import io.partdb.client.ReadConsistency;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

public final class PartDbApp {

    static final String VERSION = Optional.ofNullable(PartDbApp.class.getPackage().getImplementationVersion()).orElse("development");

    private static final String ROOT_USAGE = """
        Usage: partdb <namespace> <command> [options]

        Namespaces:
          server      Run a PartDB server node
          kv          Read and write keys
          cluster     Inspect cluster state
          help        Show this help message
          version     Show version information

        Examples:
          partdb server start --node-id node1 --data-dir ./data/node1
          partdb kv put hello world --endpoints localhost:8101
          partdb kv get hello
          partdb kv scan a z --limit 10 --output json
          partdb cluster status
          partdb cluster members

        Run "partdb <namespace> --help" for namespace commands.
        """;

    private static final String SERVER_USAGE = """
        Usage: partdb server <command>

        Commands:
          start     Start a PartDB server node
        """;

    private static final String KV_USAGE = """
        Usage: partdb kv <command>

        Commands:
          get       Read a key
          put       Write a key
          delete    Delete a key
          scan      Read a range of keys
        """;

    private static final String CLUSTER_USAGE = """
        Usage: partdb cluster <command>

        Commands:
          status     Show the status of the first endpoint's node
          members    List cluster members
        """;

    private static final String GET_USAGE = """
        Usage: partdb kv get <key> [options]

        Options:
              --local              Read local state without a linearizable read barrier
        """ + ClientOptions.USAGE;

    private static final String PUT_USAGE = """
        Usage: partdb kv put <key> <value> [options]

        Options:
        """ + ClientOptions.USAGE;

    private static final String DELETE_USAGE = """
        Usage: partdb kv delete <key> [options]

        Options:
        """ + ClientOptions.USAGE;

    private static final String SCAN_USAGE = """
        Usage: partdb kv scan [<start> [<end>]] [options]

        Reads keys from start (inclusive) to end (exclusive).

        Options:
              --limit <n>          Return at most n keys (default: no limit)
              --local              Read local state without a linearizable read barrier
        """ + ClientOptions.USAGE;

    private static final String STATUS_USAGE = """
        Usage: partdb cluster status [options]

        Options:
        """ + ClientOptions.USAGE;

    private static final String MEMBERS_USAGE = """
        Usage: partdb cluster members [options]

        Options:
        """ + ClientOptions.USAGE;

    private PartDbApp() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        return parse(args, System.getenv()).execute(out, err);
    }

    static AppCommand parse(String[] values, Map<String, String> environment) {
        var args = new Args(values);
        if (!args.hasNext()) {
            return new AppCommand.Help(ROOT_USAGE);
        }
        String namespace = args.next();
        return switch (namespace) {
            case "server" -> args.hasNext() ? switch (args.next()) {
                case "start" -> StartCommand.parse(args);
                case "help", "--help", "-h" -> new AppCommand.Help(SERVER_USAGE);
                case String other -> new AppCommand.UsageError("unknown server command: " + other, SERVER_USAGE);
            } : new AppCommand.Help(SERVER_USAGE);
            case "kv" -> args.hasNext() ? switch (args.next()) {
                case "get" -> client(args, environment, GET_USAGE, 1, 1, (positionals, flags) ->
                    new ClientCommand.Action.Get(positionals.getFirst(), flags.consistency));
                case "put" -> client(args, environment, PUT_USAGE, 2, 2, (positionals, _) ->
                    new ClientCommand.Action.Put(positionals.get(0), positionals.get(1)));
                case "delete" -> client(args, environment, DELETE_USAGE, 1, 1, (positionals, _) ->
                    new ClientCommand.Action.Delete(positionals.getFirst()));
                case "scan" -> client(args, environment, SCAN_USAGE, 0, 2, (positionals, flags) -> new ClientCommand.Action.Scan(
                    positionals.isEmpty() ? "" : positionals.get(0),
                    positionals.size() < 2 ? Optional.empty() : Optional.of(positionals.get(1)),
                    flags.limit,
                    flags.consistency
                ));
                case "help", "--help", "-h" -> new AppCommand.Help(KV_USAGE);
                case String other -> new AppCommand.UsageError("unknown kv command: " + other, KV_USAGE);
            } : new AppCommand.Help(KV_USAGE);
            case "cluster" -> args.hasNext() ? switch (args.next()) {
                case "status" -> client(args, environment, STATUS_USAGE, 0, 0, (_, _) -> new ClientCommand.Action.Status());
                case "members" -> client(args, environment, MEMBERS_USAGE, 0, 0, (_, _) -> new ClientCommand.Action.Members());
                case "help", "--help", "-h" -> new AppCommand.Help(CLUSTER_USAGE);
                case String other -> new AppCommand.UsageError("unknown cluster command: " + other, CLUSTER_USAGE);
            } : new AppCommand.Help(CLUSTER_USAGE);
            case "help", "--help", "-h" -> new AppCommand.Help(ROOT_USAGE);
            case "version", "--version", "-v" -> new AppCommand.Version(VERSION);
            default -> new AppCommand.UsageError("unknown namespace: " + namespace, ROOT_USAGE);
        };
    }

    private static final class Flags {
        private ReadConsistency consistency = ReadConsistency.LINEARIZABLE;
        private OptionalInt limit = OptionalInt.empty();
    }

    @FunctionalInterface
    private interface ActionFactory {
        ClientCommand.Action create(List<String> positionals, Flags flags);
    }

    private static AppCommand client(Args args, Map<String, String> environment, String usage, int minArgs, int maxArgs, ActionFactory factory) {
        var options = new ClientOptions.Parser(environment);
        var flags = new Flags();
        var positionals = new ArrayList<String>();
        try {
            while (args.hasNext()) {
                String arg = args.next();
                switch (arg) {
                    case "--help", "-h" -> {
                        return new AppCommand.Help(usage);
                    }
                    case "--local" -> flags.consistency = ReadConsistency.LOCAL;
                    case "--limit" -> {
                        int limit = args.intValue("--limit");
                        if (limit <= 0) {
                            throw new IllegalArgumentException("--limit must be positive");
                        }
                        flags.limit = OptionalInt.of(limit);
                    }
                    default -> {
                        if (!options.accept(arg, args)) {
                            if (arg.startsWith("-")) {
                                throw new IllegalArgumentException("unknown option: " + arg);
                            }
                            positionals.add(arg);
                        }
                    }
                }
            }
            if (positionals.size() < minArgs || positionals.size() > maxArgs) {
                throw new IllegalArgumentException(minArgs == maxArgs
                    ? "expected " + minArgs + " argument(s), got " + positionals.size()
                    : "expected " + minArgs + " to " + maxArgs + " arguments, got " + positionals.size());
            }
            return new ClientCommand(factory.create(positionals, flags), options.build());
        } catch (IllegalArgumentException e) {
            return new AppCommand.UsageError(e.getMessage(), usage);
        }
    }
}
