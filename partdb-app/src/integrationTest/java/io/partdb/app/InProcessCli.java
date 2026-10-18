package io.partdb.app;

import io.partdb.app.testing.CommandResult;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

final class InProcessCli {
    private InProcessCli() {
    }

    static CommandResult run(String... args) {
        var outBytes = new ByteArrayOutputStream();
        var errBytes = new ByteArrayOutputStream();
        try (
            var out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
            var err = new PrintStream(errBytes, true, StandardCharsets.UTF_8)
        ) {
            int exitCode = PartDbApp.parse(args, Map.of()).execute(out, err).code();
            return new CommandResult(exitCode, outBytes.toString(StandardCharsets.UTF_8), errBytes.toString(StandardCharsets.UTF_8));
        }
    }
}
