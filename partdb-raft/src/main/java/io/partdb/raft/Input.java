package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface Input {
    record Tick() implements Input {}

    record Propose(long id, Bytes command) implements Input {
        public Propose {
            Objects.requireNonNull(command, "command must not be null");
        }
    }

    record ReadIndex(long id) implements Input {}

    record ChangeMembership(long id, MembershipChange change) implements Input {
        public ChangeMembership {
            Objects.requireNonNull(change, "change must not be null");
        }
    }

    record TransferLeadership(long id, NodeId target) implements Input {
        public TransferLeadership {
            Objects.requireNonNull(target, "target must not be null");
        }
    }

    record Receive(NodeId from, Message message) implements Input {
        public Receive {
            Objects.requireNonNull(from, "from must not be null");
            Objects.requireNonNull(message, "message must not be null");
        }
    }

    record InstallSnapshot(NodeId from, long term, SnapshotMetadata snapshot) implements Input {
        public InstallSnapshot {
            Objects.requireNonNull(from, "from must not be null");
            Validation.nonNegative(term, "term");
            Objects.requireNonNull(snapshot, "snapshot must not be null");
        }
    }

    record Persisted(long index, long term) implements Input {
        public Persisted {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
        }
    }

    record SnapshotSent(NodeId peer) implements Input {
        public SnapshotSent {
            Objects.requireNonNull(peer, "peer must not be null");
        }
    }

    record SnapshotFailed(NodeId peer) implements Input {
        public SnapshotFailed {
            Objects.requireNonNull(peer, "peer must not be null");
        }
    }

    record Unreachable(NodeId peer) implements Input {
        public Unreachable {
            Objects.requireNonNull(peer, "peer must not be null");
        }
    }
}
