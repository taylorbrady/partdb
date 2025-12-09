package io.partdb.server;

import com.google.protobuf.ByteString;
import io.partdb.raft.RaftNode;
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

    public CompletableFuture<Void> put(byte[] key, byte[] value, long leaseId) {
        var tracked = pending.track();
        var command = Command.newBuilder()
            .setRequestId(tracked.requestId())
            .setPut(Put.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .setLeaseId(leaseId))
            .build();
        return propose(tracked, command);
    }

    public CompletableFuture<Void> delete(byte[] key) {
        var tracked = pending.track();
        var command = Command.newBuilder()
            .setRequestId(tracked.requestId())
            .setDelete(Delete.newBuilder()
                .setKey(ByteString.copyFrom(key)))
            .build();
        return propose(tracked, command);
    }

    private CompletableFuture<Void> propose(PendingRequests.Tracked tracked, Command command) {
        if (!raftNode.isLeader()) {
            pending.cancel(tracked.requestId());
            return CompletableFuture.failedFuture(
                new NotLeaderException(raftNode.leaderId().orElse(null))
            );
        }
        raftNode.propose(command.toByteArray());
        return tracked.future();
    }
}
