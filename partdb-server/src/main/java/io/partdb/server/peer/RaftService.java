package io.partdb.server.peer;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.Envelope;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.RaftProto;
import io.partdb.server.peer.proto.RaftServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

final class RaftService extends RaftServiceGrpc.RaftServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RaftService.class);
    static final Metadata.Key<String> SENDER_KEY = Metadata.Key.of("x-raft-sender-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<String> SENDER = Context.key("raft-sender");

    private final String nodeId;
    private final RaftTransport.Receiver receiver;

    RaftService(String nodeId, RaftTransport.Receiver receiver) {
        this.nodeId = nodeId;
        this.receiver = receiver;
    }

    static ServerInterceptor senderInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next
            ) {
                String sender = headers.get(SENDER_KEY);
                if (sender == null || sender.isBlank()) {
                    call.close(Status.UNAUTHENTICATED.withDescription("missing raft sender"), new Metadata());
                    return new ServerCall.Listener<>() {};
                }
                return Contexts.interceptCall(Context.current().withValue(SENDER, sender), call, headers, next);
            }
        };
    }

    @Override
    public StreamObserver<RaftProto.RaftMessage> send(StreamObserver<RaftProto.SendResponse> responses) {
        String from = SENDER.get();
        return new StreamObserver<>() {
            @Override
            public void onNext(RaftProto.RaftMessage message) {
                receiver.receive(new Envelope(from, nodeId, RaftProtoMapper.fromProto(message)));
            }

            @Override
            public void onError(Throwable error) {
                log.atDebug().addKeyValue("from", from).addKeyValue("error", error.getMessage()).log("Raft stream closed");
            }

            @Override
            public void onCompleted() {
                responses.onNext(RaftProto.SendResponse.getDefaultInstance());
                responses.onCompleted();
            }
        };
    }

    @Override
    public StreamObserver<RaftProto.InstallSnapshotRequest> installSnapshot(StreamObserver<RaftProto.InstallSnapshotResponse> responses) {
        String from = SENDER.get();
        return new StreamObserver<>() {
            private RaftTransport.SnapshotReceiver snapshot;
            private boolean failed;

            @Override
            public void onNext(RaftProto.InstallSnapshotRequest request) {
                if (failed) {
                    return;
                }
                try {
                    switch (request.getPayloadCase()) {
                        case HEADER -> {
                            if (snapshot != null) {
                                throw new IllegalArgumentException("duplicate snapshot header");
                            }
                            snapshot = receiver.receiveSnapshot(from, RaftProtoMapper.fromProto(request.getHeader()));
                        }
                        case CHUNK -> {
                            if (snapshot == null) {
                                throw new IllegalArgumentException("snapshot chunk received before its header");
                            }
                            snapshot.write(request.getChunk().asReadOnlyByteBuffer());
                        }
                        case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("snapshot payload not set");
                    }
                } catch (IOException | RuntimeException e) {
                    fail(e);
                }
            }

            @Override
            public void onError(Throwable error) {
                if (snapshot != null) {
                    snapshot.abort();
                }
                log.atDebug().addKeyValue("from", from).addKeyValue("error", error.getMessage()).log("Snapshot stream failed");
            }

            @Override
            public void onCompleted() {
                if (failed) {
                    return;
                }
                if (snapshot == null) {
                    fail(new IllegalArgumentException("snapshot stream ended without a header"));
                    return;
                }
                try {
                    snapshot.complete();
                } catch (IOException | RuntimeException e) {
                    fail(e);
                    return;
                }
                responses.onNext(RaftProto.InstallSnapshotResponse.getDefaultInstance());
                responses.onCompleted();
            }

            private void fail(Exception error) {
                failed = true;
                if (snapshot != null) {
                    snapshot.abort();
                }
                log.atDebug().addKeyValue("from", from).addKeyValue("error", error.getMessage()).log("Snapshot install failed");
                responses.onError(Status.INTERNAL.withDescription(error.getMessage()).withCause(error).asRuntimeException());
            }
        };
    }
}
