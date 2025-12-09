package io.partdb.storage;

import io.partdb.common.Timestamp;

public sealed interface Entry permits Entry.Put, Entry.Tombstone {

    Slice key();

    Timestamp timestamp();

    record Put(Slice key, Timestamp timestamp, Slice value) implements Entry {}

    record Tombstone(Slice key, Timestamp timestamp) implements Entry {}
}
