package io.partdb.app;

import java.util.Map;
import java.util.Optional;

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
          partdb kv put hello there --if-revision 7
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

    private PartDbApp() {
    }

    public static void main(String[] args) {
        var exit = parse(args, System.getenv()).execute(System.out, System.err);
        if (exit != ExitCode.OK) {
            System.exit(exit.code());
        }
    }

    static AppCommand parse(String[] values, Map<String, String> environment) {
        var args = new Args(values);
        if (!args.hasNext()) {
            return new AppCommand.Help(ROOT_USAGE);
        }
        String namespace = args.next();
        return switch (namespace) {
            case "server" -> server(args);
            case "kv" -> KvCommand.parse(args, environment);
            case "cluster" -> ClusterCommand.parse(args, environment);
            case "help", "--help", "-h" -> new AppCommand.Help(ROOT_USAGE);
            case "version", "--version", "-v" -> new AppCommand.Version(VERSION);
            default -> new AppCommand.UsageError("unknown namespace: " + namespace, ROOT_USAGE);
        };
    }

    private static AppCommand server(Args args) {
        if (!args.hasNext()) {
            return new AppCommand.Help(SERVER_USAGE);
        }
        return switch (args.next()) {
            case "start" -> StartCommand.parse(args);
            case "help", "--help", "-h" -> new AppCommand.Help(SERVER_USAGE);
            case String other -> new AppCommand.UsageError("unknown server command: " + other, SERVER_USAGE);
        };
    }
}
