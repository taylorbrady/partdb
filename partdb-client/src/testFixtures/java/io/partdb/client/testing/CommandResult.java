package io.partdb.client.testing;

public record CommandResult(int exitCode, String stdout, String stderr) {
}
