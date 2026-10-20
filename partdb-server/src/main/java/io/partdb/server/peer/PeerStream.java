package io.partdb.server.peer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.partdb.raft.Message;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.PeerMessage;
import io.partdb.server.peer.proto.RaftPeerGrpc;
import io.partdb.server.peer.proto.SendResponse;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayDeque;
import java.util.Queue;

import static java.util.Map.entry;

final class PeerStream {

    private static final Logger log = System.getLogger(PeerStream.class.getName());

    private enum Connection { UNKNOWN, CONNECTED, UNREACHABLE }

    private final NodeId peer;
    private final RaftPeerGrpc.RaftPeerStub stub;
    private final RaftTransport.Receiver receiver;
    private final long capacityBytes;
    private final Queue<PeerMessage> queue = new ArrayDeque<>();
    private long queuedBytes;
    private ClientCallStreamObserver<PeerMessage> stream;
    private Connection connection = Connection.UNKNOWN;
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
                log.log(Level.WARNING, "Dropping messages for a peer that is not keeping up", entry("peer", peer), entry("queuedBytes", queuedBytes));
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
                            connected();
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
                disconnected(call, PeerErrors.describe(error));
            }

            @Override
            public void onCompleted() {
                disconnected(call, "stream closed by peer");
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

    private void connected() {
        if (connection != Connection.CONNECTED) {
            connection = Connection.CONNECTED;
            log.log(Level.INFO, "Connected to peer", entry("peer", peer));
        }
    }

    private void disconnected(ClientCallStreamObserver<PeerMessage> call, String reason) {
        synchronized (this) {
            if (stream != call) {
                return;
            }
            stream = null;
            queue.clear();
            queuedBytes = 0;
            if (connection != Connection.UNREACHABLE && !closed) {
                connection = Connection.UNREACHABLE;
                log.log(Level.WARNING, "Peer is unreachable", entry("peer", peer), entry("reason", reason));
            }
        }
        receiver.unreachable(peer);
    }
}
