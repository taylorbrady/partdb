package io.partdb.raft;

public record Options(
    int electionTicks,
    int heartbeatTicks,
    long maxAppendBytes,
    int maxInflightAppends,
    long maxApplyBytes
) {
    public Options {
        Validation.positive(electionTicks, "electionTicks");
        Validation.positive(heartbeatTicks, "heartbeatTicks");
        Validation.positive(maxAppendBytes, "maxAppendBytes");
        Validation.positive(maxInflightAppends, "maxInflightAppends");
        Validation.positive(maxApplyBytes, "maxApplyBytes");
        if (heartbeatTicks >= electionTicks) {
            throw new IllegalArgumentException("heartbeatTicks must be less than electionTicks");
        }
    }

    public static Options defaults() {
        return new Options(10, 3, 1 << 20, 256, 64 << 20);
    }
}
