package io.partdb.common.exception;

import java.util.Optional;

public final class NotLeaderException extends RuntimeException {
    private final Optional<String> leaderHint;

    public NotLeaderException(Optional<String> leaderHint) {
        super("Not the leader" + leaderHint.map(h -> ", leader hint: " + h).orElse(""));
        this.leaderHint = leaderHint;
    }

    public Optional<String> leaderHint() {
        return leaderHint;
    }
}
