package io.partdb.server;

import com.google.protobuf.ByteString;
import io.partdb.server.raft.RaftNode;
import io.partdb.server.command.proto.CommandProto.Command;
import io.partdb.server.command.proto.CommandProto.Delete;
import io.partdb.server.command.proto.CommandProto.Put;

import java.util.concurrent.CompletableFuture;

public final class Proposer {
    private final RaftNode raftNode;

    public Proposer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public CompletableFuture<Long> put(byte[] key, byte[] value, long leaseId) {
        var command = Command.newBuilder()
            .setPut(Put.newBuilder()
                .setKey(ByteString.copyFrom(key))
                .setValue(ByteString.copyFrom(value))
                .setLeaseId(leaseId))
            .build();
        return raftNode.propose(command.toByteArray())
            .thenCompose(raftNode::waitForApplied);
    }

    public CompletableFuture<Long> delete(byte[] key) {
        var command = Command.newBuilder()
            .setDelete(Delete.newBuilder()
                .setKey(ByteString.copyFrom(key)))
            .build();
        return raftNode.propose(command.toByteArray())
            .thenCompose(raftNode::waitForApplied);
    }

    public CompletableFuture<Long> propose(Command.Builder commandBuilder) {
        var command = commandBuilder.build();
        return raftNode.propose(command.toByteArray())
            .thenCompose(raftNode::waitForApplied);
    }
}
