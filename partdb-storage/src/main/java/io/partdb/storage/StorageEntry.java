package io.partdb.storage;

public record StorageEntry(Slice key, Slice value, long revision) {}
