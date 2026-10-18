package io.partdb.server.peer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.partdb.raft.Message;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.PeerProto.PeerMessage;
import io.partdb.server.peer.proto.PeerProto.SendResponse;
import io.partdb.server.peer.proto.RaftPeerGrpc;

import java.util.ArrayDeque;
import java.util.Queue;

final class PeerStream {

    private final NodeId peer;
    private final RaftPeerGrpc.RaftPeerStub stub;
    private final RaftTransport.Receiver receiver;
    private final long capacityBytes;
    private final Queue<PeerMessage> queue = new ArrayDeque<>();
    private long queuedBytes;
    private ClientCallStreamObserver<PeerMessage> stream;
    private boolean closed;

    PeerStream(NodeId peer, RaftPeerGrpc.RaftPeerStub stub, RaftTransport.Receiver receiver, long capacityBytes) {
        this.peer = peer;
        this.stub = stub;
        this.receiver = receiver;
        this.capacityBytes = capacityBytes;
    }

    void send(Message message) {
        var proto = RaftProtoMapper.toProto(message);
        boolean overflow;
        synchronized (this) {
            if (closed) {
                return;
            }
            overflow = queuedBytes + proto.getSerializedSize() > capacityBytes && !queue.isEmpty();
            if (overflow) {
                reset();
            } else {
                queue.add(proto);
                queuedBytes += proto.getSerializedSize();
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
        queuedBytes = 0;
        if (stream != null) {
            stream.onCompleted();
            stream = null;
        }
    }

    private void reset() {
        queue.clear();
        queuedBytes = 0;
        if (stream != null) {
            stream.cancel("peer stream overflowed", null);
            stream = null;
        }
    }

    private void open() {
        stub.send(new ClientResponseObserver<PeerMessage, SendResponse>() {
            private ClientCallStreamObserver<PeerMessage> call;

            @Override
            public void beforeStart(ClientCallStreamObserver<PeerMessage> requests) {
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
            public void onNext(SendResponse value) {
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
            var next = queue.poll();
            queuedBytes -= next.getSerializedSize();
            stream.onNext(next);
        }
    }

    private void disconnected(ClientCallStreamObserver<PeerMessage> call) {
        synchronized (this) {
            if (stream != call) {
                return;
            }
            stream = null;
            queue.clear();
            queuedBytes = 0;
        }
        receiver.unreachable(peer);
    }
}
