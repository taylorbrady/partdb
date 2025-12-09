package io.partdb.storage;

import io.partdb.common.ByteArray;

import java.util.ArrayList;
import java.util.List;

public record WriteBatch(List<BatchEntry> entries) {

    public WriteBatch {
        entries = List.copyOf(entries);
    }

    public static Builder builder() {
        return new Builder();
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public static final class Builder {

        private final List<BatchEntry> entries = new ArrayList<>();

        public Builder put(ByteArray key, ByteArray value) {
            entries.add(new BatchEntry.Put(key, value));
            return this;
        }

        public Builder delete(ByteArray key) {
            entries.add(new BatchEntry.Delete(key));
            return this;
        }

        public WriteBatch build() {
            return new WriteBatch(entries);
        }
    }

    public sealed interface BatchEntry permits BatchEntry.Put, BatchEntry.Delete {

        ByteArray key();

        record Put(ByteArray key, ByteArray value) implements BatchEntry {}

        record Delete(ByteArray key) implements BatchEntry {}
    }
}
