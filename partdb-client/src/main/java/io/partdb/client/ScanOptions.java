package io.partdb.client;

import java.util.Objects;

public record ScanOptions(int limit, ReadConsistency consistency) {
    public static final ScanOptions DEFAULT = new ScanOptions(0, ReadConsistency.LINEARIZABLE);

    public ScanOptions {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        Objects.requireNonNull(consistency, "consistency must not be null");
    }

    public ScanOptions withLimit(int limit) {
        return new ScanOptions(limit, consistency);
    }

    public ScanOptions withConsistency(ReadConsistency consistency) {
        return new ScanOptions(limit, consistency);
    }
}
