package io.partdb.consensus;

import io.partdb.raft.RaftMessage;
import io.partdb.raft.transport.RaftPeerTransport;

import java.util.concurrent.CompletableFuture;

final class SingleNodeRaftPeerTransport implements RaftPeerTransport {
    @Override
    public void start(RpcHandler handler) {
    }

    @Override
    public CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request) {
        return CompletableFuture.failedFuture(new UnsupportedOperationException("single-node transport"));
    }

    @Override
    public void close() {
    }
}
