package io.partdb.client;

import io.partdb.bytes.Bytes;

import java.util.Objects;
import java.util.Optional;

public record KeyRange(Bytes start, Optional<Bytes> end) {
    public KeyRange {
        Objects.requireNonNull(start, "start must not be null");
        Objects.requireNonNull(end, "end must not be null");
        end.ifPresent(limit -> {
            if (start.compareTo(limit) > 0) {
                throw new IllegalArgumentException("start must not be after end");
            }
        });
    }

    public static KeyRange all() {
        return new KeyRange(Bytes.EMPTY, Optional.empty());
    }

    public static KeyRange from(Bytes start) {
        return new KeyRange(start, Optional.empty());
    }

    public static KeyRange until(Bytes end) {
        return new KeyRange(Bytes.EMPTY, Optional.of(end));
    }

    public static KeyRange between(Bytes start, Bytes end) {
        return new KeyRange(start, Optional.of(end));
    }
}
