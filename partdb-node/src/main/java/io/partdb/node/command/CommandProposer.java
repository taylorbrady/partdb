package io.partdb.node.command;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.ConsensusNode;

import java.util.concurrent.CompletableFuture;

public final class CommandProposer {
    private final ConsensusNode consensus;

    public CommandProposer(ConsensusNode consensus) {
        this.consensus = consensus;
    }

    public CompletableFuture<Long> put(Bytes key, Bytes value, long leaseId) {
        return propose(new PartDbCommand.Put(key, value, leaseId));
    }

    public CompletableFuture<Long> delete(Bytes key) {
        return propose(new PartDbCommand.Delete(key));
    }

    public CompletableFuture<Long> propose(PartDbCommand command) {
        return consensus.commit(PartDbCommandCodec.encode(command));
    }
}
