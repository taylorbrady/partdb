package io.partdb.common;

public record Entry(Slice key, Slice value, long revision) {}
