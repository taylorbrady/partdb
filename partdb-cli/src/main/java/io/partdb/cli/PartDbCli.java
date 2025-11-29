package io.partdb.cli;

import java.io.PrintStream;
import java.util.Arrays;

public final class PartDbCli {
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
        out.println("  start    Start a PartDB server node");
        out.println("  help     Show this help message");
        out.println("  version  Show version information");
        out.println();
        out.println("Run 'partdb <command> --help' for more information on a command.");
    }
}
