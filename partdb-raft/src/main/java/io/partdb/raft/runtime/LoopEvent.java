package io.partdb.raft.runtime;

import io.partdb.raft.Input;
import io.partdb.raft.NodeId;

import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

sealed interface LoopEvent {
    record Step(Input input) implements LoopEvent {}

    record Submit(CompletableFuture<?> future, Runnable register) implements LoopEvent {}

    record SnapshotReceived(NodeId from, long term, SnapshotInfo info, Path payload) implements LoopEvent {}

    record SnapshotDelivered(NodeId peer, boolean delivered) implements LoopEvent {}

    record SnapshotGenerated(Path payload, long index, long size, int checksum, Optional<Throwable> failure) implements LoopEvent {}

    record Shutdown() implements LoopEvent {}
}
