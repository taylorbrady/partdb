package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;
import java.util.Optional;

public record KeyRange(Optional<Bytes> startInclusive, Optional<Bytes> endExclusive) {
    public KeyRange {
        startInclusive = Objects.requireNonNull(startInclusive, "startInclusive must not be null");
        endExclusive = Objects.requireNonNull(endExclusive, "endExclusive must not be null");
        if (startInclusive.isPresent() && endExclusive.isPresent()
            && startInclusive.orElseThrow().compareTo(endExclusive.orElseThrow()) > 0) {
            throw new IllegalArgumentException("startInclusive must be <= endExclusive");
        }
    }

    public static KeyRange all() {
        return new KeyRange(Optional.empty(), Optional.empty());
    }

    public static KeyRange from(Bytes startInclusive) {
        return new KeyRange(Optional.of(Objects.requireNonNull(startInclusive, "startInclusive must not be null")), Optional.empty());
    }

    public static KeyRange until(Bytes endExclusive) {
        return new KeyRange(Optional.empty(), Optional.of(Objects.requireNonNull(endExclusive, "endExclusive must not be null")));
    }

    public static KeyRange between(Bytes startInclusive, Bytes endExclusive) {
        return new KeyRange(
            Optional.of(Objects.requireNonNull(startInclusive, "startInclusive must not be null")),
            Optional.of(Objects.requireNonNull(endExclusive, "endExclusive must not be null"))
        );
    }
}
