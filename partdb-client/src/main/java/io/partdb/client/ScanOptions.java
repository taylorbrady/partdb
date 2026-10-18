package io.partdb.client;

import java.util.Objects;
import java.util.OptionalInt;

public record ScanOptions(OptionalInt limit, ReadConsistency consistency) {
    public static final ScanOptions DEFAULT = new ScanOptions(OptionalInt.empty(), ReadConsistency.LINEARIZABLE);

    public ScanOptions {
        Objects.requireNonNull(limit, "limit must not be null");
        Objects.requireNonNull(consistency, "consistency must not be null");
        if (limit.isPresent() && limit.getAsInt() <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
    }

    public ScanOptions withLimit(int limit) {
        return new ScanOptions(OptionalInt.of(limit), consistency);
    }

    public ScanOptions withConsistency(ReadConsistency consistency) {
        return new ScanOptions(limit, consistency);
    }
}
