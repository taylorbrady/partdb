package io.partdb.node.runtime;

import io.partdb.consensus.ConsensusRuntime;
import io.partdb.consensus.ProposalResult;
import io.partdb.node.command.KvCommand;
import io.partdb.node.command.KvCommandCodec;
import io.partdb.node.command.KvCommandResult;
import io.partdb.node.command.KvCommandResultCodec;
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
            .thenApply(CommandProposer::decode);
    }

    private static WriteResult decode(ProposalResult result) {
        KvCommandResult decoded = KvCommandResultCodec.decode(result.result());
        return switch (result) {
            case ProposalResult.Applied _ -> switch (decoded) {
                case KvCommandResult.Applied(long revision) -> new WriteResult(revision);
            };
            case ProposalResult.Rejected _ -> throw new IllegalStateException("Command was rejected: " + decoded);
        };
    }
}
