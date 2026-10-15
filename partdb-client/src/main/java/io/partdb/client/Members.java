package io.partdb.client;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record Members(List<Member> members, Optional<String> leaderId) {
    public Members {
        members = List.copyOf(Objects.requireNonNull(members, "members must not be null"));
        Objects.requireNonNull(leaderId, "leaderId must not be null");
    }
}
