package io.partdb.consensus;

import io.partdb.bytes.Bytes;

import java.util.concurrent.CompletableFuture;

public interface ConsensusRuntime extends AutoCloseable {
    CompletableFuture<ProposalResult> propose(Bytes data);

    CompletableFuture<ReadBarrier> readBarrier();

    ConsensusStatus status();

    ConsensusMembership membership();

    @Override
    void close();
}
