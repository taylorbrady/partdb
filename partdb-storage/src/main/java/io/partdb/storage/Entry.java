package io.partdb.storage;

import io.partdb.common.ByteArray;

public sealed interface Entry permits Entry.Data, Entry.Tombstone {

    ByteArray key();

    record Data(ByteArray key, ByteArray value) implements Entry {}

    record Tombstone(ByteArray key) implements Entry {}
}
