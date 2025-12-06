package io.partdb.server;

import java.util.Optional;

public final class NotLeaderException extends RuntimeException {
    private final String leaderId;

    public NotLeaderException(String leaderId) {
        super(leaderId != null ? "Not leader, leader is: " + leaderId : "Not leader, leader unknown");
        this.leaderId = leaderId;
    }

    public Optional<String> leaderId() {
        return Optional.ofNullable(leaderId);
    }
}
