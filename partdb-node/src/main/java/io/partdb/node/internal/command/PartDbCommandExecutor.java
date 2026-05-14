package io.partdb.node.internal.command;

import io.partdb.consensus.ProposalResult;
import io.partdb.consensus.ConsensusRuntime;

import java.util.concurrent.CompletableFuture;

public final class PartDbCommandExecutor {
    private final ConsensusRuntime consensus;

    public PartDbCommandExecutor(ConsensusRuntime consensus) {
        this.consensus = consensus;
    }

    public <R> CompletableFuture<R> execute(ReplicatedCommand<R> command) {
        return consensus.propose(PartDbCommandCodec.encode(command.payload()))
            .thenApply(PartDbCommandExecutor::decode)
            .thenApply(command::mapResult);
    }

    private static PartDbCommandResult decode(ProposalResult result) {
        PartDbCommandResult decoded = PartDbCommandResultCodec.decode(result.result());
        return switch (result) {
            case ProposalResult.Applied _ -> decoded;
            case ProposalResult.Rejected _ -> throw new IllegalStateException("Command was rejected: " + decoded);
        };
    }
}
