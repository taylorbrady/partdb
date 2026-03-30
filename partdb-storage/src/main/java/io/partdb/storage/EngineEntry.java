package io.partdb.storage;

record EngineEntry(Slice key, Slice value, long revision) {}
