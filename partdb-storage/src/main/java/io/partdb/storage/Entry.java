package io.partdb.storage;

import io.partdb.common.ByteArray;
import io.partdb.common.Timestamp;

public sealed interface Entry permits Entry.Put, Entry.Tombstone {

    ByteArray key();

    Timestamp timestamp();

    record Put(ByteArray key, Timestamp timestamp, ByteArray value) implements Entry {}

    record Tombstone(ByteArray key, Timestamp timestamp) implements Entry {}
}
