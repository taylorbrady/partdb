package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;

import java.util.concurrent.CompletableFuture;

public interface ConsensusRuntime extends AutoCloseable {
    CompletableFuture<CommitResult> commit(Bytes data);

    CompletableFuture<Long> linearizableBarrier();

    ConsensusStatus status();

    ClusterMembership membership();

    @Override
    void close();
}
