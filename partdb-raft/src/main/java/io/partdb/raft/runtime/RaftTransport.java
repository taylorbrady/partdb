package io.partdb.raft.runtime;

import io.partdb.raft.Envelope;
import io.partdb.raft.NodeId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public interface RaftTransport {

    void start(Receiver receiver);

    void send(Envelope envelope);

    CompletionStage<Void> sendSnapshot(NodeId to, long term, SnapshotSource snapshot);

    interface Receiver {
        void receive(Envelope envelope);

        SnapshotSink receiveSnapshot(NodeId from, long term, SnapshotInfo snapshot) throws IOException;

        void unreachable(NodeId peer);
    }

    interface SnapshotSource extends AutoCloseable {
        SnapshotInfo info();

        FileChannel channel();

        @Override
        void close();
    }

    interface SnapshotSink {
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
            public CompletionStage<Void> sendSnapshot(NodeId to, long term, SnapshotSource snapshot) {
                snapshot.close();
                return CompletableFuture.failedFuture(new IllegalStateException("a standalone node has no peers"));
            }
        };
    }
}
