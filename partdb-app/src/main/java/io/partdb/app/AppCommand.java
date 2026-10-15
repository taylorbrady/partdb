package io.partdb.app;

import java.io.PrintStream;
import java.util.Objects;

sealed interface AppCommand permits AppCommand.Help, AppCommand.Version, AppCommand.UsageError, StartCommand, ClientCommand {

    int OK = 0;
    int FAILED = 1;
    int USAGE = 2;
    int NOT_FOUND = 3;

    int execute(PrintStream out, PrintStream err);

    record Help(String usage) implements AppCommand {
        @Override
        public int execute(PrintStream out, PrintStream err) {
            out.print(usage);
            return OK;
        }
    }

    record Version(String version) implements AppCommand {
        @Override
        public int execute(PrintStream out, PrintStream err) {
            out.println("partdb " + version);
            return OK;
        }
    }

    record UsageError(String message, String usage) implements AppCommand {
        public UsageError {
            Objects.requireNonNull(message, "message must not be null");
            Objects.requireNonNull(usage, "usage must not be null");
        }

        @Override
        public int execute(PrintStream out, PrintStream err) {
            err.println("Error: " + message);
            err.println();
            err.print(usage);
            return USAGE;
        }
    }
}
