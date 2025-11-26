package io.partdb.storage;

import io.partdb.common.Entry;

@FunctionalInterface
public interface CompactionFilter {

    boolean shouldRetain(Entry entry);

    static CompactionFilter retainAll() {
        return entry -> true;
    }
}
