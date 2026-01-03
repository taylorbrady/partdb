package io.partdb.server.raft;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftTransport;
import io.partdb.server.raft.proto.RaftProto;
import io.partdb.server.raft.proto.RaftServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

final class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RaftServiceImpl.class);
    static final Metadata.Key<String> SENDER_ID_KEY =
        Metadata.Key.of("x-raft-sender-id", Metadata.ASCII_STRING_MARSHALLER);
    static final Context.Key<String> SENDER_ID_CONTEXT_KEY = Context.key("sender-id");

    private final RaftTransport.RpcHandler handler;

    RaftServiceImpl(RaftTransport.RpcHandler handler) {
        this.handler = handler;
    }

    static ServerInterceptor senderIdInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                    ServerCall<ReqT, RespT> call,
                    Metadata headers,
                    ServerCallHandler<ReqT, RespT> next) {
                String senderId = headers.get(SENDER_ID_KEY);
                Context ctx = Context.current().withValue(SENDER_ID_CONTEXT_KEY, senderId);
                return Contexts.interceptCall(ctx, call, headers, next);
            }
        };
    }

    private String getSenderId() {
        String senderId = SENDER_ID_CONTEXT_KEY.get();
        return senderId != null ? senderId : "unknown";
    }

    @Override
    public void requestVote(RaftProto.RequestVoteRequest request,
                            StreamObserver<RaftProto.RequestVoteResponse> responseObserver) {
        String from = getSenderId();
        RaftMessage.RequestVote msg = ProtoConverters.fromProto(request);

        handler.handle(from, msg)
            .whenComplete((response, ex) -> {
                if (ex != null) {
                    log.atDebug()
                        .addKeyValue("rpc", "RequestVote")
                        .addKeyValue("from", from)
                        .addKeyValue("error", ex.getMessage())
                        .log("RPC failed");
                    responseObserver.onError(ex);
                } else {
                    var resp = (RaftMessage.RequestVoteResponse) response;
                    responseObserver.onNext(ProtoConverters.toProto(resp));
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void preVote(RaftProto.PreVoteRequest request,
                        StreamObserver<RaftProto.PreVoteResponse> responseObserver) {
        String from = getSenderId();
        RaftMessage.PreVote msg = ProtoConverters.fromProto(request);

        handler.handle(from, msg)
            .whenComplete((response, ex) -> {
                if (ex != null) {
                    log.atDebug()
                        .addKeyValue("rpc", "PreVote")
                        .addKeyValue("from", from)
                        .addKeyValue("error", ex.getMessage())
                        .log("RPC failed");
                    responseObserver.onError(ex);
                } else {
                    var resp = (RaftMessage.PreVoteResponse) response;
                    responseObserver.onNext(ProtoConverters.toProto(resp));
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public void appendEntries(RaftProto.AppendEntriesRequest request,
                              StreamObserver<RaftProto.AppendEntriesResponse> responseObserver) {
        String from = getSenderId();
        RaftMessage.AppendEntries msg = ProtoConverters.fromProto(request);

        handler.handle(from, msg)
            .whenComplete((response, ex) -> {
                if (ex != null) {
                    log.atDebug()
                        .addKeyValue("rpc", "AppendEntries")
                        .addKeyValue("from", from)
                        .addKeyValue("error", ex.getMessage())
                        .log("RPC failed");
                    responseObserver.onError(ex);
                } else {
                    var resp = (RaftMessage.AppendEntriesResponse) response;
                    responseObserver.onNext(ProtoConverters.toProto(resp));
                    responseObserver.onCompleted();
                }
            });
    }

    @Override
    public StreamObserver<RaftProto.InstallSnapshotRequest> installSnapshot(
            StreamObserver<RaftProto.InstallSnapshotResponse> responseObserver) {

        return new StreamObserver<>() {
            private RaftProto.SnapshotHeader header;
            private final ByteArrayOutputStream dataBuffer = new ByteArrayOutputStream();

            @Override
            public void onNext(RaftProto.InstallSnapshotRequest request) {
                switch (request.getPayloadCase()) {
                    case HEADER -> header = request.getHeader();
                    case CHUNK -> {
                        var chunk = request.getChunk();
                        try {
                            chunk.getData().writeTo(dataBuffer);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    case PAYLOAD_NOT_SET -> {}
                }
            }

            @Override
            public void onError(Throwable t) {
                log.atDebug()
                    .addKeyValue("rpc", "InstallSnapshot")
                    .addKeyValue("error", t.getMessage())
                    .log("Stream error");
            }

            @Override
            public void onCompleted() {
                if (header == null) {
                    responseObserver.onError(new IllegalStateException("No snapshot header received"));
                    return;
                }

                RaftMessage.InstallSnapshot snapshot = ProtoConverters.fromSnapshotHeader(
                    header,
                    dataBuffer.toByteArray()
                );

                handler.handle(header.getLeaderId(), snapshot)
                    .whenComplete((response, ex) -> {
                        if (ex != null) {
                            log.atDebug()
                                .addKeyValue("rpc", "InstallSnapshot")
                                .addKeyValue("from", header.getLeaderId())
                                .addKeyValue("error", ex.getMessage())
                                .log("RPC failed");
                            responseObserver.onError(ex);
                        } else {
                            var resp = (RaftMessage.InstallSnapshotResponse) response;
                            responseObserver.onNext(ProtoConverters.toProto(resp));
                            responseObserver.onCompleted();
                        }
                    });
            }
        };
    }

    @Override
    public void readIndex(RaftProto.ReadIndexRequest request,
                          StreamObserver<RaftProto.ReadIndexResponse> responseObserver) {
        String from = getSenderId();
        RaftMessage.ReadIndex msg = ProtoConverters.fromProto(request);

        handler.handle(from, msg)
            .whenComplete((response, ex) -> {
                if (ex != null) {
                    log.atDebug()
                        .addKeyValue("rpc", "ReadIndex")
                        .addKeyValue("from", from)
                        .addKeyValue("error", ex.getMessage())
                        .log("RPC failed");
                    responseObserver.onError(ex);
                } else {
                    var resp = (RaftMessage.ReadIndexResponse) response;
                    responseObserver.onNext(ProtoConverters.toProto(resp));
                    responseObserver.onCompleted();
                }
            });
    }
}
