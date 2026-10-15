package io.partdb.client;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

public record RetryPolicy(Duration initialBackoff, Duration maxBackoff) {
    public static final RetryPolicy DEFAULT = new RetryPolicy(Duration.ofMillis(20), Duration.ofMillis(500));

    public RetryPolicy {
        Objects.requireNonNull(initialBackoff, "initialBackoff must not be null");
        Objects.requireNonNull(maxBackoff, "maxBackoff must not be null");
        if (initialBackoff.isNegative() || maxBackoff.compareTo(initialBackoff) < 0) {
            throw new IllegalArgumentException("backoff must satisfy 0 <= initialBackoff <= maxBackoff");
        }
    }

    Duration backoff(int attempt) {
        long ceiling = Math.min(maxBackoff.toNanos(), initialBackoff.toNanos() << Math.min(attempt, 20));
        return Duration.ofNanos(ceiling <= 0 ? 0 : ThreadLocalRandom.current().nextLong(ceiling / 2, ceiling + 1));
    }
}
