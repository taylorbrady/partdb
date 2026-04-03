package io.partdb.consensus.transport;

import java.util.concurrent.CompletableFuture;

public interface ConsensusTransport extends AutoCloseable {
    void start(RpcHandler handler);

    CompletableFuture<ConsensusRpc.Response> send(String to, ConsensusRpc.Request request);

    @Override
    void close();

    @FunctionalInterface
    interface RpcHandler {
        CompletableFuture<ConsensusRpc.Response> handle(String from, ConsensusRpc.Request request);
    }
}
