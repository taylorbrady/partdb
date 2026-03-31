package io.partdb.node.command;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.command.proto.CommandProto.Delete;
import io.partdb.node.command.proto.CommandProto.Put;
import io.partdb.node.raft.RaftNode;

import java.util.concurrent.CompletableFuture;

public final class CommandProposer {
    private final RaftNode raftNode;

    public CommandProposer(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    public CompletableFuture<Long> put(Bytes key, Bytes value, long leaseId) {
        var command = Command.newBuilder()
            .setPut(Put.newBuilder()
                .setKey(ByteString.copyFrom(key.asReadOnlyByteBuffer()))
                .setValue(ByteString.copyFrom(value.asReadOnlyByteBuffer()))
                .setLeaseId(leaseId))
            .build();
        return raftNode.propose(Bytes.copyOf(command.toByteArray()))
            .thenCompose(result -> raftNode.waitForApplied(result.index()));
    }

    public CompletableFuture<Long> delete(Bytes key) {
        var command = Command.newBuilder()
            .setDelete(Delete.newBuilder()
                .setKey(ByteString.copyFrom(key.asReadOnlyByteBuffer())))
            .build();
        return raftNode.propose(Bytes.copyOf(command.toByteArray()))
            .thenCompose(result -> raftNode.waitForApplied(result.index()));
    }

    public CompletableFuture<Long> propose(Command.Builder commandBuilder) {
        var command = commandBuilder.build();
        return raftNode.propose(Bytes.copyOf(command.toByteArray()))
            .thenCompose(result -> raftNode.waitForApplied(result.index()));
    }
}
