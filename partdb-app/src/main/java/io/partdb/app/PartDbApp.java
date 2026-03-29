package io.partdb.app;

import java.io.PrintStream;
import java.util.Arrays;

public final class PartDbApp {
    private static final String VERSION = "0.1.0-SNAPSHOT";

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0) {
            printUsage(out);
            return 0;
        }

        String command = args[0];
        String[] commandArgs = Arrays.copyOfRange(args, 1, args.length);

        return switch (command) {
            case "start" -> StartCommand.run(commandArgs, out, err);
            case "get" -> GetCommand.run(commandArgs, out, err);
            case "put" -> PutCommand.run(commandArgs, out, err);
            case "delete" -> DeleteCommand.run(commandArgs, out, err);
            case "status" -> StatusCommand.run(commandArgs, out, err);
            case "member" -> MemberCommand.run(commandArgs, out, err);
            case "help", "--help", "-h" -> {
                printUsage(out);
                yield 0;
            }
            case "version", "--version", "-v" -> {
                out.println("partdb " + VERSION);
                yield 0;
            }
            default -> {
                err.println("Unknown command: " + command);
                err.println();
                printUsage(err);
                yield 1;
            }
        };
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb <command> [options]");
        out.println();
        out.println("Commands:");
        out.println("  start                 Start a PartDB server node");
        out.println("  get <key>             Get a value by key");
        out.println("  put <key> <value>     Put a key-value pair");
        out.println("  delete <key>          Delete a key");
        out.println("  status                Show node status");
        out.println("  member <subcommand>   Manage cluster members");
        out.println("  help                  Show this help message");
        out.println("  version               Show version information");
        out.println();
        out.println("Options:");
        out.println("  -e, --endpoint <host:port>  Server endpoint (default: localhost:8101)");
        out.println("  -o, --output <format>       Output format: text, json (default: text)");
        out.println();
        out.println("Examples:");
        out.println("  partdb start --node-id node1 --data-dir ./data");
        out.println("  partdb put hello world");
        out.println("  partdb get hello");
        out.println("  partdb status");
        out.println("  partdb member list");
    }
}
