package io.partdb.storage.compaction;

import java.util.Objects;
import java.util.Set;

public record ReservationToken(
    Set<Long> sstableIds,
    int sourceLevel,
    int targetLevel,
    KeyRange keyRange
) {

    public ReservationToken {
        Objects.requireNonNull(sstableIds, "sstableIds");
        Objects.requireNonNull(keyRange, "keyRange");
        sstableIds = Set.copyOf(sstableIds);

        if (sstableIds.isEmpty()) {
            throw new IllegalArgumentException("sstableIds cannot be empty");
        }
        if (sourceLevel < 0) {
            throw new IllegalArgumentException("sourceLevel must be non-negative");
        }
        if (targetLevel < 0) {
            throw new IllegalArgumentException("targetLevel must be non-negative");
        }
    }
}
