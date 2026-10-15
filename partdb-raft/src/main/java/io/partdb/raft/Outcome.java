package io.partdb.raft;

import java.util.Objects;

public sealed interface Outcome {
    long id();

    record Appended(long id, long index, long term) implements Outcome {}

    record ReadReady(long id, long index) implements Outcome {}

    record Transferred(long id) implements Outcome {}

    record Rejected(long id, Rejection reason) implements Outcome {
        public Rejected {
            Objects.requireNonNull(reason, "reason must not be null");
        }
    }
}
