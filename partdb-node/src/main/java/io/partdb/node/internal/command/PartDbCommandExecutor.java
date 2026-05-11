package io.partdb.node.internal.command;

import io.partdb.consensus.CommitResult;
import io.partdb.consensus.ConsensusRuntime;

import java.util.concurrent.CompletableFuture;

public final class PartDbCommandExecutor {
    private final ConsensusRuntime consensus;

    public PartDbCommandExecutor(ConsensusRuntime consensus) {
        this.consensus = consensus;
    }

    public <R> CompletableFuture<R> execute(ReplicatedCommand<R> command) {
        return consensus.commit(PartDbCommandCodec.encode(command.payload()))
            .thenApply(PartDbCommandExecutor::decode)
            .thenApply(command::mapResult);
    }

    private static PartDbCommandResult decode(CommitResult result) {
        PartDbCommandResult decoded = PartDbCommandResultCodec.decode(result.result());
        return switch (result) {
            case CommitResult.Applied _ -> decoded;
            case CommitResult.Rejected _ -> throw new IllegalStateException("Command was rejected: " + decoded);
        };
    }
}
