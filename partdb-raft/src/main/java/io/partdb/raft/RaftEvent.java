package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface RaftEvent {
    record Tick() implements RaftEvent {}
    record Propose(Bytes data) implements RaftEvent {
        public Propose {
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }
    record Receive(String from, RaftMessage message) implements RaftEvent {}
    record ReadIndex(Bytes context) implements RaftEvent {
        public ReadIndex {
            context = Objects.requireNonNull(context, "context must not be null");
        }
    }

    record ChangeConfiguration(ConfigurationChange change) implements RaftEvent {
        public ChangeConfiguration {
            Objects.requireNonNull(change, "change must not be null");
        }
    }
}
