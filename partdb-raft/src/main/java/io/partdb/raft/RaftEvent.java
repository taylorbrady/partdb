package io.partdb.raft;

public sealed interface RaftEvent {
    record Tick() implements RaftEvent {}
    record Propose(byte[] data) implements RaftEvent {}
    record Receive(String from, RaftMessage message) implements RaftEvent {}
    record ReadIndex(byte[] context) implements RaftEvent {}
    record ChangeConfig(ConfigChange change) implements RaftEvent {}
}
