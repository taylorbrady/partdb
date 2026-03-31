package io.partdb.app;

import java.util.Objects;

record ErrorCommand(String message, String usage) implements AppCommand {
    ErrorCommand {
        message = Objects.requireNonNull(message, "message must not be null");
    }

    @Override
    public int execute(CliRuntime runtime) {
        runtime.err().println("Error: " + message);
        if (usage != null && !usage.isBlank()) {
            runtime.err().println();
            runtime.err().print(usage);
        }
        return 1;
    }
}
