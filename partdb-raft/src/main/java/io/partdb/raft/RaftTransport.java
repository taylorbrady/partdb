package io.partdb.raft;

import java.util.concurrent.CompletableFuture;

public interface RaftTransport extends AutoCloseable {
    void start(RpcHandler handler);

    CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request);

    @Override
    void close();

    @FunctionalInterface
    interface RpcHandler {
        CompletableFuture<RaftMessage.Response> handle(String from, RaftMessage.Request request);
    }
}
