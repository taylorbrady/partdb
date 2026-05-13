package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface RaftInput {
    record Tick() implements RaftInput {}
    record EntryProposed(Bytes data) implements RaftInput {
        public EntryProposed {
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }
    record MessageReceived(String from, RaftMessage message) implements RaftInput {}
    record ReadIndexRequested(Bytes context) implements RaftInput {
        public ReadIndexRequested {
            context = Objects.requireNonNull(context, "context must not be null");
        }
    }

    record MembershipChangeProposed(MembershipChange change) implements RaftInput {
        public MembershipChangeProposed {
            Objects.requireNonNull(change, "change must not be null");
        }
    }

    record EntriesPersisted(long index, long term) implements RaftInput {
        public EntriesPersisted {
            if (index < 0) {
                throw new IllegalArgumentException("index must be non-negative");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record SnapshotPersisted() implements RaftInput {}

    record Applied(long index) implements RaftInput {
        public Applied {
            if (index < 0) {
                throw new IllegalArgumentException("index must be non-negative");
            }
        }
    }

    record ReplayCommitted() implements RaftInput {}
}
