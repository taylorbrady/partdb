package io.partdb.raft;

public record RaftConfig(
    int electionTimeoutMin,
    int electionTimeoutMax,
    int heartbeatInterval,
    int maxEntriesPerAppend
) {
    public RaftConfig {
        if (electionTimeoutMin <= 0) {
            throw new IllegalArgumentException("electionTimeoutMin must be positive");
        }
        if (electionTimeoutMax <= electionTimeoutMin) {
            throw new IllegalArgumentException("electionTimeoutMax must be greater than electionTimeoutMin");
        }
        if (heartbeatInterval <= 0) {
            throw new IllegalArgumentException("heartbeatInterval must be positive");
        }
        if (heartbeatInterval >= electionTimeoutMin) {
            throw new IllegalArgumentException("heartbeatInterval must be less than electionTimeoutMin");
        }
        if (maxEntriesPerAppend <= 0) {
            throw new IllegalArgumentException("maxEntriesPerAppend must be positive");
        }
    }

    public static RaftConfig defaults() {
        return new RaftConfig(10, 20, 3, 100);
    }
}
