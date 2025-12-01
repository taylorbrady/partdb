package io.partdb.ctl;

import java.io.PrintStream;
import java.util.Arrays;

public final class PartDbCtl {

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
            case "get" -> GetCommand.run(commandArgs, out, err);
            case "put" -> PutCommand.run(commandArgs, out, err);
            case "delete" -> DeleteCommand.run(commandArgs, out, err);
            case "help", "--help", "-h" -> {
                printUsage(out);
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
        out.println("Usage: partdbctl <command> [options]");
        out.println();
        out.println("Commands:");
        out.println("  get <key>              Get a value by key");
        out.println("  put <key> <value>      Put a key-value pair");
        out.println("  delete <key>           Delete a key");
        out.println("  help                   Show this help message");
        out.println();
        out.println("Options:");
        out.println("  --endpoint <host:port>  Server endpoint (default: localhost:8101)");
        out.println();
        out.println("Examples:");
        out.println("  partdbctl put hello world");
        out.println("  partdbctl get hello");
        out.println("  partdbctl delete hello");
        out.println("  partdbctl get hello --endpoint node1:8101");
    }
}
