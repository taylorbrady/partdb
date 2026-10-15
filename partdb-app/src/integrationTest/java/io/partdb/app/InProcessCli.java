package io.partdb.app;

import io.partdb.client.testing.CommandResult;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

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
            int exitCode = PartDbApp.run(args, out, err);
            return new CommandResult(
                exitCode,
                outBytes.toString(StandardCharsets.UTF_8),
                errBytes.toString(StandardCharsets.UTF_8)
            );
        }
    }
}
