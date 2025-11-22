package io.partdb.raft;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;

public record RaftConfig(
    String nodeId,
    List<String> peers,
    Path dataDirectory,
    long electionTimeoutMinMs,
    long electionTimeoutMaxMs,
    long heartbeatIntervalMs,
    int maxProposalQueueSize,
    int maxEntriesPerAppend,
    long snapshotThresholdBytes,
    Duration minSnapshotInterval
) {
    public RaftConfig {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(peers, "peers must not be null");
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(minSnapshotInterval, "minSnapshotInterval must not be null");

        if (electionTimeoutMinMs <= 0) {
            throw new IllegalArgumentException("electionTimeoutMinMs must be positive");
        }
        if (electionTimeoutMaxMs <= electionTimeoutMinMs) {
            throw new IllegalArgumentException("electionTimeoutMaxMs must be greater than electionTimeoutMinMs");
        }
        if (heartbeatIntervalMs <= 0) {
            throw new IllegalArgumentException("heartbeatIntervalMs must be positive");
        }
        if (maxProposalQueueSize <= 0) {
            throw new IllegalArgumentException("maxProposalQueueSize must be positive");
        }
        if (maxEntriesPerAppend <= 0) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be positive");
        }
        if (snapshotThresholdBytes <= 0) {
            throw new IllegalArgumentException("snapshotThresholdBytes must be positive");
        }
        if (minSnapshotInterval.isNegative() || minSnapshotInterval.isZero()) {
            throw new IllegalArgumentException("minSnapshotInterval must be positive");
        }

        peers = List.copyOf(peers);
    }

    public static RaftConfig create(String nodeId, List<String> peers, Path dataDirectory) {
        return new RaftConfig(
            nodeId,
            peers,
            dataDirectory,
            150,
            300,
            50,
            1000,
            100,
            1024 * 1024 * 1024,
            Duration.ofMinutes(1)
        );
    }
}
