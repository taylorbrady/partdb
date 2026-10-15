package io.partdb.raft;

import java.util.Objects;

public record Envelope(String from, String to, RaftMessage message) {
    public Envelope {
        Validation.nodeId(from, "from");
        Validation.nodeId(to, "to");
        Objects.requireNonNull(message, "message must not be null");
    }
}
