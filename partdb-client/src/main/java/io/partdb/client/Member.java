package io.partdb.client;

import java.util.Objects;

public record Member(String nodeId, MemberRole role) {
    public Member {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
    }
}
