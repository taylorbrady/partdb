package io.partdb.app;

import java.util.Objects;

record VersionCommand(String version) implements AppCommand {
    VersionCommand {
        version = Objects.requireNonNull(version, "version must not be null");
    }

    @Override
    public int execute(CliRuntime runtime) {
        runtime.out().println("partdb " + version);
        return 0;
    }
}
