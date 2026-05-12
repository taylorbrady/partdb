package io.partdb.consensus;

import io.partdb.raft.RaftMessage;

import java.util.concurrent.CompletableFuture;

public interface RaftPeerTransport extends AutoCloseable {
    void start(RpcHandler handler);

    CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request);

    @Override
    void close();

    @FunctionalInterface
    interface RpcHandler {
        CompletableFuture<RaftMessage.Response> handle(String from, RaftMessage.Request request);
    }
}
