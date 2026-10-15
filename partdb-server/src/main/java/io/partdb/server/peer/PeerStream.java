package io.partdb.server.peer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.RaftProto;
import io.partdb.server.peer.proto.RaftServiceGrpc;

import java.util.ArrayDeque;
import java.util.Queue;

final class PeerStream {

    private final String peer;
    private final RaftServiceGrpc.RaftServiceStub stub;
    private final RaftTransport.Receiver receiver;
    private final int capacity;
    private final Queue<RaftProto.RaftMessage> queue = new ArrayDeque<>();
    private ClientCallStreamObserver<RaftProto.RaftMessage> stream;
    private boolean closed;

    PeerStream(String peer, RaftServiceGrpc.RaftServiceStub stub, RaftTransport.Receiver receiver, int capacity) {
        this.peer = peer;
        this.stub = stub;
        this.receiver = receiver;
        this.capacity = capacity;
    }

    void send(RaftMessage message) {
        boolean overflow;
        synchronized (this) {
            if (closed) {
                return;
            }
            overflow = queue.size() >= capacity;
            if (!overflow) {
                queue.add(RaftProtoMapper.toProto(message));
                if (stream == null) {
                    open();
                }
                drain();
            }
        }
        if (overflow) {
            receiver.unreachable(peer);
        }
    }

    synchronized void close() {
        closed = true;
        queue.clear();
        if (stream != null) {
            stream.onCompleted();
            stream = null;
        }
    }

    private void open() {
        stub.send(new ClientResponseObserver<RaftProto.RaftMessage, RaftProto.SendResponse>() {
            private ClientCallStreamObserver<RaftProto.RaftMessage> call;

            @Override
            public void beforeStart(ClientCallStreamObserver<RaftProto.RaftMessage> requests) {
                call = requests;
                stream = requests;
                requests.setOnReadyHandler(() -> {
                    synchronized (PeerStream.this) {
                        if (stream == call) {
                            drain();
                        }
                    }
                });
            }

            @Override
            public void onNext(RaftProto.SendResponse value) {
            }

            @Override
            public void onError(Throwable error) {
                disconnected(call);
            }

            @Override
            public void onCompleted() {
                disconnected(call);
            }
        });
    }

    private void drain() {
        while (stream != null && stream.isReady() && !queue.isEmpty()) {
            stream.onNext(queue.poll());
        }
    }

    private void disconnected(ClientCallStreamObserver<RaftProto.RaftMessage> call) {
        synchronized (this) {
            if (stream != call) {
                return;
            }
            stream = null;
            queue.clear();
        }
        receiver.unreachable(peer);
    }
}
