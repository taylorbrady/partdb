package io.partdb.node.command;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.node.command.proto.CommandProto.Command;
import io.partdb.node.command.proto.CommandProto.Delete;
import io.partdb.node.command.proto.CommandProto.Put;
import io.partdb.consensus.ConsensusNode;

import java.util.concurrent.CompletableFuture;

public final class CommandProposer {
    private final ConsensusNode consensus;

    public CommandProposer(ConsensusNode consensus) {
        this.consensus = consensus;
    }

    public CompletableFuture<Long> put(Bytes key, Bytes value, long leaseId) {
        var command = Command.newBuilder()
            .setPut(Put.newBuilder()
                .setKey(ByteString.copyFrom(key.asReadOnlyByteBuffer()))
                .setValue(ByteString.copyFrom(value.asReadOnlyByteBuffer()))
                .setLeaseId(leaseId))
            .build();
        return consensus.commit(Bytes.copyOf(command.toByteArray()));
    }

    public CompletableFuture<Long> delete(Bytes key) {
        var command = Command.newBuilder()
            .setDelete(Delete.newBuilder()
                .setKey(ByteString.copyFrom(key.asReadOnlyByteBuffer())))
            .build();
        return consensus.commit(Bytes.copyOf(command.toByteArray()));
    }

    public CompletableFuture<Long> propose(Command.Builder commandBuilder) {
        var command = commandBuilder.build();
        return consensus.commit(Bytes.copyOf(command.toByteArray()));
    }
}
