package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftMembership;

import java.util.concurrent.CompletableFuture;

public interface ConsensusRuntime extends AutoCloseable {
    CompletableFuture<ProposalResult> propose(Bytes data);

    CompletableFuture<ReadBarrier> readBarrier();

    ConsensusStatus status();

    RaftMembership membership();

    @Override
    void close();
}
