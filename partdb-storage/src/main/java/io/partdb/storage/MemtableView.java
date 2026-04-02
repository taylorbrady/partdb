package io.partdb.storage;

import java.util.List;
import java.util.Objects;

record MemtableView(Memtable activeMemtable, List<ImmutableMemtable> immutableMemtables) {

    MemtableView {
        Objects.requireNonNull(activeMemtable, "activeMemtable");
        immutableMemtables = List.copyOf(Objects.requireNonNull(immutableMemtables, "immutableMemtables"));
    }
}
