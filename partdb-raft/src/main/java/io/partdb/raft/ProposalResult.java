package io.partdb.raft;

public record ProposalResult(long index, long term) {}
