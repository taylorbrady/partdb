package io.partdb.storage;

record StorageEntry(Slice key, Slice value, long revision) {}
