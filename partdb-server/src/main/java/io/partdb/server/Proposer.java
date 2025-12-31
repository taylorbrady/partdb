package io.partdb.server;

import com.google.protobuf.ByteString;
import io.partdb.raft.RaftException;
import io.partdb.server.raft.RaftNode;
import io.partdb.server.command.proto.CommandProto.Command;
import io.partdb.server.command.proto.CommandProto.Delete;
import io.partdb.server.command.proto.CommandProto.Put;

import java.util.concurrent.CompletableFuture;

public final class Proposer {
    private final RaftNode raftNode;
    private final PendingRequests pending;

    public Proposer(RaftNode raftNode, PendingRequests pending) {
        this.raftNode = raftNode;
        this.pending = pending;
    }

    public boolean isLeader() {
        return raftNode.isLeader();
    }

    public CompletableFuture<Void> put(byte[] key, byte[] value, long leaseId) {
        return propose(Command.newBuilder()
            .setPut(Put.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .setLeaseId(leaseId)));
    }

    public CompletableFuture<Void> delete(byte[] key) {
        return propose(Command.newBuilder()
            .setDelete(Delete.newBuilder()
                .setKey(ByteString.copyFrom(key))));
    }

    public CompletableFuture<Void> readIndex() {
        return raftNode.linearizableBarrier();
    }

    public CompletableFuture<Void> propose(Command.Builder commandBuilder) {
        var tracked = pending.track();
        var command = commandBuilder
            .setRequestId(tracked.requestId())
            .build();

        if (!raftNode.isLeader()) {
            pending.cancel(tracked.requestId());
            return CompletableFuture.failedFuture(
                new RaftException.NotLeader(raftNode.leaderId().orElse(null))
            );
        }
        raftNode.propose(command.toByteArray());
        return tracked.future();
    }
}
