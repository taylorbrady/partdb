package io.partdb.raft.runtime;

import io.partdb.raft.Envelope;
import io.partdb.raft.RaftMessage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface RaftTransport extends AutoCloseable {

    void start(Receiver receiver);

    void send(Envelope envelope);

    CompletionStage<Void> sendSnapshot(String to, RaftMessage.InstallSnapshot request, Path payload);

    @Override
    void close();

    interface Receiver {
        void receive(Envelope envelope);

        SnapshotReceiver receiveSnapshot(String from, RaftMessage.InstallSnapshot request) throws IOException;

        void unreachable(String peer);
    }

    interface SnapshotReceiver {
        void write(ByteBuffer chunk) throws IOException;

        void complete() throws IOException;

        void abort();
    }

    static RaftTransport standalone() {
        return new RaftTransport() {
            @Override
            public void start(Receiver receiver) {
            }

            @Override
            public void send(Envelope envelope) {
                throw new IllegalStateException("a standalone node has no peers");
            }

            @Override
            public CompletionStage<Void> sendSnapshot(String to, RaftMessage.InstallSnapshot request, Path payload) {
                return CompletableFuture.failedFuture(new IllegalStateException("a standalone node has no peers"));
            }

            @Override
            public void close() {
            }
        };
    }
}
