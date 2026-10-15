package io.partdb.raft;

import java.util.Objects;
import java.util.Optional;

public sealed interface Rejection {
    record NotLeader(Optional<NodeId> leader) implements Rejection {
        public NotLeader {
            Objects.requireNonNull(leader, "leader must not be null");
        }
    }

    record TransferInProgress() implements Rejection {}

    record ChangeInProgress() implements Rejection {}

    record TimedOut() implements Rejection {}

    record Aborted() implements Rejection {}

    record Invalid(String reason) implements Rejection {
        public Invalid {
            Objects.requireNonNull(reason, "reason must not be null");
        }
    }
}
