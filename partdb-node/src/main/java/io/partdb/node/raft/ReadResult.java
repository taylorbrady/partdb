package io.partdb.node.raft;

public record ReadResult(long index, long term) {}
