package io.partdb.node.transport;

import java.util.concurrent.CompletableFuture;

public interface ConsensusTransport extends AutoCloseable {
    void start(RpcHandler handler);

    CompletableFuture<ConsensusMessage.Response> send(String to, ConsensusMessage.Request request);

    @Override
    void close();

    @FunctionalInterface
    interface RpcHandler {
        CompletableFuture<ConsensusMessage.Response> handle(String from, ConsensusMessage.Request request);
    }
}
