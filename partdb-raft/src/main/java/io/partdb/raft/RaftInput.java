package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface RaftInput {
    record Tick() implements RaftInput {}

    record Propose(long id, Bytes command) implements RaftInput {
        public Propose {
            Objects.requireNonNull(command, "command must not be null");
        }
    }

    record Receive(String from, RaftMessage message) implements RaftInput {
        public Receive {
            Validation.nodeId(from, "from");
            Objects.requireNonNull(message, "message must not be null");
        }
    }

    record ReadIndex(long id) implements RaftInput {}

    record ChangeMembership(long id, MembershipChange change) implements RaftInput {
        public ChangeMembership {
            Objects.requireNonNull(change, "change must not be null");
        }
    }

    record TransferLeadership(String target) implements RaftInput {
        public TransferLeadership {
            Validation.nodeId(target, "target");
        }
    }

    record Persisted(long index, long term) implements RaftInput {
        public Persisted {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
        }
    }

    record SnapshotSent(String peer) implements RaftInput {
        public SnapshotSent {
            Validation.nodeId(peer, "peer");
        }
    }

    record SnapshotFailed(String peer) implements RaftInput {
        public SnapshotFailed {
            Validation.nodeId(peer, "peer");
        }
    }

    record Unreachable(String peer) implements RaftInput {
        public Unreachable {
            Validation.nodeId(peer, "peer");
        }
    }
}
