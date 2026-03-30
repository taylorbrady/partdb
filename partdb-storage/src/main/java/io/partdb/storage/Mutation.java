package io.partdb.storage;

sealed interface Mutation permits Mutation.Put, Mutation.Tombstone {

    Slice key();

    long revision();

    default int sizeInBytes() {
        int base = 1 + 8 + 4 + key().length() + 4;
        return switch (this) {
            case Put put -> base + put.value().length();
            case Tombstone _ -> base;
        };
    }

    record Put(Slice key, Slice value, long revision) implements Mutation {}

    record Tombstone(Slice key, long revision) implements Mutation {}
}
