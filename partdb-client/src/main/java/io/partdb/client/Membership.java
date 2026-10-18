package io.partdb.client;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record Membership(List<Member> members, Optional<String> leaderId) {
    public Membership {
        members = List.copyOf(Objects.requireNonNull(members, "members must not be null"));
        Objects.requireNonNull(leaderId, "leaderId must not be null");
    }
}
