package io.partdb.storage;

import io.partdb.common.Slice;

public sealed interface Mutation permits Mutation.Put, Mutation.Tombstone {

    Slice key();

    long revision();

    record Put(Slice key, Slice value, long revision) implements Mutation {}

    record Tombstone(Slice key, long revision) implements Mutation {}
}
