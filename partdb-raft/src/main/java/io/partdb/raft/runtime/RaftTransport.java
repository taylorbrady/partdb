package io.partdb.raft.runtime;

import io.partdb.raft.RaftMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public interface RaftTransport extends AutoCloseable {
    void start(Inbound inbound);

    CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request);

    CompletableFuture<RaftMessage.InstallSnapshotResponse> sendSnapshot(
        String to,
        RaftMessage.InstallSnapshot request,
        Path payload
    );

    @Override
    void close();

    interface Inbound {
        CompletableFuture<RaftMessage.Response> receive(String from, RaftMessage.Request request);

        SnapshotReceiver receiveSnapshot(String from, RaftMessage.InstallSnapshot request) throws IOException;
    }

    interface SnapshotReceiver {
        void write(ByteBuffer chunk) throws IOException;

        CompletableFuture<RaftMessage.InstallSnapshotResponse> complete() throws IOException;

        void abort();
    }

    static RaftTransport standalone() {
        return new RaftTransport() {
            @Override
            public void start(Inbound inbound) {
            }

            @Override
            public CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request) {
                return CompletableFuture.failedFuture(new IllegalStateException("standalone node has no peers"));
            }

            @Override
            public CompletableFuture<RaftMessage.InstallSnapshotResponse> sendSnapshot(
                String to,
                RaftMessage.InstallSnapshot request,
                Path payload
            ) {
                return CompletableFuture.failedFuture(new IllegalStateException("standalone node has no peers"));
            }

            @Override
            public void close() {
            }
        };
    }
}
