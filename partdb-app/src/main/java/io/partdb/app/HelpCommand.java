package io.partdb.app;

import java.util.Objects;

record HelpCommand(String text) implements AppCommand {
    HelpCommand {
        text = Objects.requireNonNull(text, "text must not be null");
    }

    @Override
    public int execute(CliRuntime runtime) {
        runtime.out().print(text);
        return 0;
    }
}
