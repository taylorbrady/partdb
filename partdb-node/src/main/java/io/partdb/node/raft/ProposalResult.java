package io.partdb.node.raft;

public record ProposalResult(long index, long term) {}
