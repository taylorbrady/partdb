package io.partdb.app;

import io.partdb.client.PartDbClient;
import io.partdb.client.PartDbException;

import java.io.PrintStream;

sealed interface ClientCommand extends AppCommand permits KvCommand, ClusterCommand {

    ClientOptions options();

    ExitCode run(PartDbClient client, PrintStream out, PrintStream err);

    @Override
    default ExitCode execute(PrintStream out, PrintStream err) {
        try (var client = options().connect()) {
            return run(client, out, err);
        } catch (PartDbException e) {
            err.println("Error: " + describe(e) + ": " + e.getMessage());
            return ExitCode.FAILED;
        }
    }

    default void print(PrintStream out, Record json, String text) {
        out.print(options().output() == OutputFormat.JSON ? Json.write(json) + "\n" : text);
    }

    private static String describe(PartDbException error) {
        return switch (error) {
            case PartDbException.Unavailable _ -> "unavailable";
            case PartDbException.Timeout _ -> "timed out";
            case PartDbException.InvalidRequest _ -> "invalid request";
            case PartDbException.OutcomeUnknown _ -> "outcome unknown";
            case PartDbException.Internal _ -> "server error";
            case PartDbException.Cancelled _ -> "cancelled";
            case PartDbException.Closed _ -> "client closed";
        };
    }
}
