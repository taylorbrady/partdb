package io.partdb.raft;

import java.util.Objects;

public sealed interface RaftEvent {
    record Tick() implements RaftEvent {}
    record Propose(byte[] data) implements RaftEvent {
        public Propose {
            Objects.requireNonNull(data, "data must not be null");
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }
    record Receive(String from, RaftMessage message) implements RaftEvent {}
    record ReadIndex(byte[] context) implements RaftEvent {
        public ReadIndex {
            Objects.requireNonNull(context, "context must not be null");
            context = context.clone();
        }

        @Override
        public byte[] context() {
            return context.clone();
        }
    }
    record ChangeConfig(ConfigChange change) implements RaftEvent {}
}
