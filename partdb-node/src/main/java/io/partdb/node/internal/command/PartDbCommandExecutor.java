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
            case CommitResult.Applied _ -> {
                if (decoded instanceof PartDbCommandResult.RejectedCommandResult) {
                    throw new IllegalStateException("Applied commit yielded rejected result: " + decoded);
                }
                yield decoded;
            }
            case CommitResult.Rejected _ -> {
                if (decoded instanceof PartDbCommandResult.AppliedCommandResult) {
                    throw new IllegalStateException("Rejected commit yielded applied result: " + decoded);
                }
                yield decoded;
            }
        };
    }
}
