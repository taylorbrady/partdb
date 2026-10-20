package io.partdb.server.peer;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.partdb.raft.Message;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.PeerMessage;
import io.partdb.server.peer.proto.RaftPeerGrpc;
import io.partdb.server.peer.proto.SendResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Queue;

final class PeerStream {

    private static final Logger log = LoggerFactory.getLogger(PeerStream.class);

    private enum Connection { UNKNOWN, CONNECTED, UNREACHABLE }

    private final NodeId self;
    private final NodeId peer;
    private final RaftPeerGrpc.RaftPeerStub stub;
    private final RaftTransport.Receiver receiver;
    private final long capacityBytes;
    private final Queue<PeerMessage> queue = new ArrayDeque<>();
    private long queuedBytes;
    private ClientCallStreamObserver<PeerMessage> stream;
    private Connection connection = Connection.UNKNOWN;
    private boolean closed;

    PeerStream(NodeId self, NodeId peer, RaftPeerGrpc.RaftPeerStub stub, RaftTransport.Receiver receiver, long capacityBytes) {
        this.self = self;
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
                log.atWarn().addKeyValue("nodeId", self).addKeyValue("peer", peer).addKeyValue("queuedBytes", queuedBytes)
                    .log("Dropping messages for a peer that is not keeping up");
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
            log.atInfo().addKeyValue("nodeId", self).addKeyValue("peer", peer).log("Connected to peer");
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
                log.atWarn().addKeyValue("nodeId", self).addKeyValue("peer", peer).addKeyValue("reason", reason).log("Peer is unreachable");
            }
        }
        receiver.unreachable(peer);
    }
}
