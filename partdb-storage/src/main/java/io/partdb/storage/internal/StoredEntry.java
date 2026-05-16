package io.partdb.storage.internal;

import io.partdb.storage.*;

sealed interface StoredEntry permits StoredEntry.Value, StoredEntry.Tombstone {

    Slice key();

    long revision();

    int encodedSizeBytes();

    record Value(Slice key, Slice value, long revision) implements StoredEntry {
        @Override
        public int encodedSizeBytes() {
            return 1 + Long.BYTES + Integer.BYTES + key.length() + Integer.BYTES + value.length();
        }
    }

    record Tombstone(Slice key, long revision) implements StoredEntry {
        @Override
        public int encodedSizeBytes() {
            return 1 + Long.BYTES + Integer.BYTES + key.length() + Integer.BYTES;
        }
    }
}
