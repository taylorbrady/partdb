package io.partdb.storage;

public record Entry(Slice key, Slice value, long revision) {}
