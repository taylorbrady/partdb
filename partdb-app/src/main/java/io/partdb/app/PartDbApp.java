package io.partdb.app;

import java.io.PrintStream;

public final class PartDbApp {
    static final String VERSION = "0.1.0-SNAPSHOT";

    private static final String ROOT_USAGE = """
        Usage: partdb <namespace> <command> [options]

        Namespaces:
          server      Manage the local PartDB server process
          kv          Run key-value operations
          cluster     Inspect cluster state and membership
          help        Show this help message
          version     Show version information

        Examples:
          partdb server start --node-id node1 --data-dir ./data/node1
          partdb kv put hello world --endpoint localhost:8101
          partdb kv get hello --endpoint localhost:8101
          partdb cluster status --endpoint localhost:8101
          partdb cluster members --endpoint localhost:8101

        Run "partdb <namespace> --help" for namespace-specific commands.
        """;

    private static final String SERVER_USAGE = """
        Usage: partdb server <command>

        Commands:
          start     Start a PartDB server node

        Run "partdb server <command> --help" for command-specific options.
        """;

    private static final String KV_USAGE = """
        Usage: partdb kv <command>

        Commands:
          get       Get a value by key
          put       Put a key-value pair
          delete    Delete a key

        Run "partdb kv <command> --help" for command-specific options.
        """;

    private static final String CLUSTER_USAGE = """
        Usage: partdb cluster <command>

        Commands:
          status     Show node status
          members    List cluster members

        Run "partdb cluster <command> --help" for command-specific options.
        """;

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        return parse(args).execute(new CliRuntime(out, err));
    }

    static AppCommand parse(String[] args) {
        Args parsedArgs = new Args(args);
        if (!parsedArgs.hasNext()) {
            return new HelpCommand(ROOT_USAGE);
        }

        String namespace = parsedArgs.next();
        return switch (namespace) {
            case "server" -> parseServer(parsedArgs);
            case "kv" -> parseKv(parsedArgs);
            case "cluster" -> parseCluster(parsedArgs);
            case "help", "--help", "-h" -> new HelpCommand(ROOT_USAGE);
            case "version", "--version", "-v" -> new VersionCommand(VERSION);
            default -> new ErrorCommand("unknown namespace: " + namespace, ROOT_USAGE);
        };
    }

    private static AppCommand parseServer(Args args) {
        if (!args.hasNext()) {
            return new HelpCommand(SERVER_USAGE);
        }

        String command = args.next();
        return switch (command) {
            case "start" -> StartCommand.parse(args);
            case "help", "--help", "-h" -> new HelpCommand(SERVER_USAGE);
            default -> new ErrorCommand("unknown server command: " + command, SERVER_USAGE);
        };
    }

    private static AppCommand parseKv(Args args) {
        if (!args.hasNext()) {
            return new HelpCommand(KV_USAGE);
        }

        String command = args.next();
        return switch (command) {
            case "get" -> GetCommand.parse(args);
            case "put" -> PutCommand.parse(args);
            case "delete" -> DeleteCommand.parse(args);
            case "help", "--help", "-h" -> new HelpCommand(KV_USAGE);
            default -> new ErrorCommand("unknown kv command: " + command, KV_USAGE);
        };
    }

    private static AppCommand parseCluster(Args args) {
        if (!args.hasNext()) {
            return new HelpCommand(CLUSTER_USAGE);
        }

        String command = args.next();
        return switch (command) {
            case "status" -> StatusCommand.parse(args);
            case "members" -> MemberCommand.parse(args);
            case "help", "--help", "-h" -> new HelpCommand(CLUSTER_USAGE);
            default -> new ErrorCommand("unknown cluster command: " + command, CLUSTER_USAGE);
        };
    }
}
