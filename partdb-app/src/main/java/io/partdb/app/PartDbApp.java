package io.partdb.app;

import java.io.PrintStream;

public final class PartDbApp {
    static final String VERSION = "0.1.0-SNAPSHOT";

    private static final String USAGE = """
        Usage: partdb <command> [options]

        Commands:
          start                 Start a PartDB server node
          get <key>             Get a value by key
          put <key> <value>     Put a key-value pair
          delete <key>          Delete a key
          status                Show node status
          member list           List cluster members
          help                  Show this help message
          version               Show version information

        Run "partdb <command> --help" for command-specific options.
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
            return new HelpCommand(USAGE);
        }

        String command = parsedArgs.next();
        return switch (command) {
            case "start" -> StartCommand.parse(parsedArgs);
            case "get" -> GetCommand.parse(parsedArgs);
            case "put" -> PutCommand.parse(parsedArgs);
            case "delete" -> DeleteCommand.parse(parsedArgs);
            case "status" -> StatusCommand.parse(parsedArgs);
            case "member" -> MemberCommand.parse(parsedArgs);
            case "help", "--help", "-h" -> new HelpCommand(USAGE);
            case "version", "--version", "-v" -> new VersionCommand(VERSION);
            default -> new ErrorCommand("unknown command: " + command, USAGE);
        };
    }
}
