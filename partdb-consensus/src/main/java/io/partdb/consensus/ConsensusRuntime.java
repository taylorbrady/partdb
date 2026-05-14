package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;

import java.util.concurrent.CompletableFuture;

public interface ConsensusRuntime extends AutoCloseable {
    CompletableFuture<ProposalResult> propose(Bytes data);

    CompletableFuture<ReadBarrier> readBarrier();

    ConsensusStatus status();

    ClusterMembership membership();

    @Override
    void close();
}
