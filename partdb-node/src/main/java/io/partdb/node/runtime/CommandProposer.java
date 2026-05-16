package io.partdb.node.runtime;

import io.partdb.consensus.ConsensusRuntime;
import io.partdb.consensus.ProposalResult;
import io.partdb.node.command.KvCommand;
import io.partdb.node.command.KvCommandCodec;
import io.partdb.node.command.KvCommandResult;
import io.partdb.node.command.KvCommandResultCodec;
import io.partdb.node.kv.TransactionResult;
import io.partdb.node.kv.WriteResult;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public final class CommandProposer {
    private final ConsensusRuntime consensus;

    public CommandProposer(ConsensusRuntime consensus) {
        this.consensus = Objects.requireNonNull(consensus, "consensus must not be null");
    }

    public CompletableFuture<WriteResult> propose(KvCommand command) {
        Objects.requireNonNull(command, "command must not be null");
        return consensus.propose(KvCommandCodec.encode(command))
            .thenApply(CommandProposer::decodeWrite);
    }

    public CompletableFuture<TransactionResult> proposeTransaction(KvCommand.CompareAndWrite command) {
        Objects.requireNonNull(command, "command must not be null");
        return consensus.propose(KvCommandCodec.encode(command))
            .thenApply(CommandProposer::decodeTransaction);
    }

    private static WriteResult decodeWrite(ProposalResult result) {
        KvCommandResult decoded = KvCommandResultCodec.decode(result.result());
        return switch (result) {
            case ProposalResult.Applied _ -> switch (decoded) {
                case KvCommandResult.Applied(long revision) -> new WriteResult(revision);
                case KvCommandResult.ConditionFailed _ -> throw new IllegalStateException("Write command failed conditions");
            };
            case ProposalResult.Rejected _ -> throw new IllegalStateException("Command was rejected: " + decoded);
        };
    }

    private static TransactionResult decodeTransaction(ProposalResult result) {
        KvCommandResult decoded = KvCommandResultCodec.decode(result.result());
        return switch (result) {
            case ProposalResult.Applied _ -> switch (decoded) {
                case KvCommandResult.Applied(long revision) -> new TransactionResult.Applied(revision);
                case KvCommandResult.ConditionFailed _ -> new TransactionResult.ConditionFailed();
            };
            case ProposalResult.Rejected _ -> throw new IllegalStateException("Command was rejected: " + decoded);
        };
    }
}
