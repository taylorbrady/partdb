package io.partdb.raft;

import java.util.Objects;

public record Envelope(NodeId from, NodeId to, Message message) {
    public Envelope {
        Objects.requireNonNull(from, "from must not be null");
        Objects.requireNonNull(to, "to must not be null");
        Objects.requireNonNull(message, "message must not be null");
    }
}
