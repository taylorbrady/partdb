package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.CommitResult;
import io.partdb.consensus.ConsensusNode;

import java.util.concurrent.CompletableFuture;

public final class CommandProposer {
    private final ConsensusNode consensus;

    public CommandProposer(ConsensusNode consensus) {
        this.consensus = consensus;
    }

    public CompletableFuture<PartDbCommandResult> put(Bytes key, Bytes value, long leaseId) {
        return propose(new PartDbCommand.Put(key, value, leaseId));
    }

    public CompletableFuture<PartDbCommandResult> delete(Bytes key) {
        return propose(new PartDbCommand.Delete(key));
    }

    public CompletableFuture<PartDbCommandResult> propose(PartDbCommand command) {
        return consensus.commit(PartDbCommandCodec.encode(command))
            .thenApply(CommandProposer::decode);
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
