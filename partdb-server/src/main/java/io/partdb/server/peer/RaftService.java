package io.partdb.server.peer;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.Envelope;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.InstallSnapshotRequest;
import io.partdb.server.peer.proto.InstallSnapshotResponse;
import io.partdb.server.peer.proto.PeerMessage;
import io.partdb.server.peer.proto.RaftPeerGrpc;
import io.partdb.server.peer.proto.SendResponse;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;

import static java.util.Map.entry;

final class RaftService extends RaftPeerGrpc.RaftPeerImplBase {

    private static final Logger log = System.getLogger(RaftService.class.getName());

    private final NodeId self;
    private final RaftTransport.Receiver receiver;

    RaftService(NodeId self, RaftTransport.Receiver receiver) {
        this.self = self;
        this.receiver = receiver;
    }

    @Override
    public StreamObserver<PeerMessage> send(StreamObserver<SendResponse> responses) {
        NodeId from = PeerHeaders.SENDER.get();
        return new StreamObserver<>() {
            @Override
            public void onNext(PeerMessage message) {
                receiver.receive(new Envelope(from, self, RaftProtoMapper.fromProto(message)));
            }

            @Override
            public void onError(Throwable error) {
                log.log(Level.DEBUG, "Peer stream closed", entry("from", from), entry("reason", PeerErrors.describe(error)));
            }

            @Override
            public void onCompleted() {
                responses.onNext(SendResponse.getDefaultInstance());
                responses.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<InstallSnapshotRequest> installSnapshot(StreamObserver<InstallSnapshotResponse> responses) {
        NodeId from = PeerHeaders.SENDER.get();
        return new StreamObserver<>() {
            private RaftTransport.SnapshotSink sink;
            private boolean failed;

            @Override
            public void onNext(InstallSnapshotRequest request) {
                if (failed) {
                    return;
                }
                try {
                    switch (request.getPayloadCase()) {
                        case HEADER -> {
                            if (sink != null) {
                                throw new IllegalArgumentException("duplicate snapshot header");
                            }
                            var header = request.getHeader();
                            sink = receiver.receiveSnapshot(from, header.getTerm(), RaftProtoMapper.fromProto(header));
                        }
                        case CHUNK -> {
                            if (sink == null) {
                                throw new IllegalArgumentException("snapshot chunk received before its header");
                            }
                            sink.write(request.getChunk().asReadOnlyByteBuffer());
                        }
                        case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("snapshot payload not set");
                    }
                } catch (IOException | RuntimeException e) {
                    fail(e);
                }
            }

            @Override
            public void onError(Throwable error) {
                if (sink != null) {
                    sink.abort();
                }
                log.log(Level.WARNING, "Snapshot transfer from peer was interrupted", entry("from", from), entry("reason", PeerErrors.describe(error)));
            }

            @Override
            public void onCompleted() {
                if (failed) {
                    return;
                }
                if (sink == null) {
                    fail(new IllegalArgumentException("snapshot stream ended without a header"));
                    return;
                }
                try {
                    sink.complete();
                } catch (IOException | RuntimeException e) {
                    fail(e);
                    return;
                }
                responses.onNext(InstallSnapshotResponse.getDefaultInstance());
                responses.onCompleted();
            }

            private void fail(Exception error) {
                failed = true;
                if (sink != null) {
                    sink.abort();
                }
                log.log(Level.WARNING, "Rejected a snapshot from peer", entry("from", from), error);
                responses.onError(Status.INTERNAL.withDescription(error.getMessage()).withCause(error).asRuntimeException());
            }
        };
    }
}
