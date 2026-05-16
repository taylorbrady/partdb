package io.partdb.server;

public interface PartDbNodeMXBean {
    String getNodeId();

    boolean isRunning();

    String getRole();

    String getLeaderId();

    long getCurrentTerm();

    long getCommitIndex();

    long getAppliedIndex();

    long getLastLeaderChangeEpochMillis();

    long getProposalCount();

    long getProposalFailureCount();
}
