package io.partdb.raft;

public record ReadResult(long index, long term) {}
