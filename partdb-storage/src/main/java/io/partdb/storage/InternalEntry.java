package io.partdb.storage;

import java.util.Optional;

sealed interface InternalEntry {

    InternalKey key();

    default Slice userKey() {
        return key().userKey();
    }

    default long revision() {
        return key().revision();
    }

    int encodedSize();

    static InternalEntry of(Mutation mutation, long revision) {
        return switch (mutation) {
            case Mutation.Put(var key, var value) -> new Value(new InternalKey(Slice.of(key), revision), Slice.of(value));
            case Mutation.Delete(var key) -> new Tombstone(new InternalKey(Slice.of(key), revision));
        };
    }

    static Optional<Entry> visible(InternalEntry entry) {
        return switch (entry) {
            case Value value -> Optional.of(value.toEntry());
            case Tombstone _ -> Optional.empty();
        };
    }

    record Value(InternalKey key, Slice value) implements InternalEntry {
        @Override
        public int encodedSize() {
            return Long.BYTES + userKey().length() + value.length() + 8;
        }

        Entry toEntry() {
            return new Entry(userKey().toBytes(), value.toBytes(), revision());
        }
    }

    record Tombstone(InternalKey key) implements InternalEntry {
        @Override
        public int encodedSize() {
            return Long.BYTES + userKey().length() + 8;
        }
    }
}
