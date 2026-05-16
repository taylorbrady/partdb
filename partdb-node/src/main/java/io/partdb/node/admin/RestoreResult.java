package io.partdb.node.admin;

public record RestoreResult(long revision) {
    public RestoreResult {
        if (revision < 0) {
            throw new IllegalArgumentException("revision must not be negative");
        }
    }
}
