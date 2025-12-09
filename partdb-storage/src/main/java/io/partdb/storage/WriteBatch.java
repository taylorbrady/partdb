package io.partdb.storage;

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

        public Builder put(byte[] key, byte[] value) {
            entries.add(new BatchEntry.Put(key.clone(), value.clone()));
            return this;
        }

        public Builder delete(byte[] key) {
            entries.add(new BatchEntry.Delete(key.clone()));
            return this;
        }

        public WriteBatch build() {
            return new WriteBatch(entries);
        }
    }

    public sealed interface BatchEntry permits BatchEntry.Put, BatchEntry.Delete {

        byte[] key();

        record Put(byte[] key, byte[] value) implements BatchEntry {}

        record Delete(byte[] key) implements BatchEntry {}
    }
}
