package io.partdb.consensus;

import java.util.Objects;
import java.util.Optional;

public record ConsensusStatus(
    String nodeId,
    ConsensusRole role,
    long term,
    Optional<String> leaderId,
    long commitIndex,
    long appliedIndex,
    long lastLeaderChangeEpochMillis,
    boolean running
) {
    public ConsensusStatus {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(role, "role must not be null");
        Objects.requireNonNull(leaderId, "leaderId must not be null");
    }
}
