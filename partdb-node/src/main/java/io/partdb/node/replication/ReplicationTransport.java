package io.partdb.node.replication;

import java.util.concurrent.CompletableFuture;

public interface ReplicationTransport extends AutoCloseable {
    void start(RpcHandler handler);

    CompletableFuture<ReplicationRpc.Response> send(String to, ReplicationRpc.Request request);

    @Override
    void close();

    @FunctionalInterface
    interface RpcHandler {
        CompletableFuture<ReplicationRpc.Response> handle(String from, ReplicationRpc.Request request);
    }
}
