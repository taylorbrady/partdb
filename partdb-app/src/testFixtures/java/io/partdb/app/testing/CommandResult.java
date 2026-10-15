package io.partdb.app.testing;

public record CommandResult(int exitCode, String stdout, String stderr) {
}
