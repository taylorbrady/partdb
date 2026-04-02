package io.partdb.storage;

sealed interface InternalEntry permits InternalEntry.Value, InternalEntry.Tombstone {

    InternalKey key();

    default Slice userKey() {
        return key().userKey();
    }

    default long revision() {
        return key().revision();
    }

    StoredEntry toStoredEntry();

    int encodedSizeBytes();

    static InternalEntry from(StoredEntry entry) {
        return switch (entry) {
            case StoredEntry.Value value -> new Value(new InternalKey(value.key(), value.revision()), value.value());
            case StoredEntry.Tombstone tombstone -> new Tombstone(new InternalKey(tombstone.key(), tombstone.revision()));
        };
    }

    record Value(InternalKey key, Slice value) implements InternalEntry {
        @Override
        public StoredEntry.Value toStoredEntry() {
            return new StoredEntry.Value(userKey(), value, revision());
        }

        @Override
        public int encodedSizeBytes() {
            return 1 + Long.BYTES + Integer.BYTES + userKey().length() + Integer.BYTES + value.length();
        }
    }

    record Tombstone(InternalKey key) implements InternalEntry {
        @Override
        public StoredEntry.Tombstone toStoredEntry() {
            return new StoredEntry.Tombstone(userKey(), revision());
        }

        @Override
        public int encodedSizeBytes() {
            return 1 + Long.BYTES + Integer.BYTES + userKey().length() + Integer.BYTES;
        }
    }
}
