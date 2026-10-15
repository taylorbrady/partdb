package io.partdb.server.peer;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.server.peer.proto.RaftProto;
import io.partdb.server.peer.proto.RaftServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

final class RaftService extends RaftServiceGrpc.RaftServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(RaftService.class);
    static final Metadata.Key<String> SENDER_KEY = Metadata.Key.of("x-raft-sender-id", Metadata.ASCII_STRING_MARSHALLER);
    private static final Context.Key<String> SENDER = Context.key("raft-sender");

    private final RaftTransport.Inbound inbound;

    RaftService(RaftTransport.Inbound inbound) {
        this.inbound = inbound;
    }

    static ServerInterceptor senderInterceptor() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call,
                Metadata headers,
                ServerCallHandler<ReqT, RespT> next
            ) {
                Context context = Context.current().withValue(SENDER, headers.get(SENDER_KEY));
                return Contexts.interceptCall(context, call, headers, next);
            }
        };
    }

    @Override
    public void requestVote(RaftProto.RequestVoteRequest request, StreamObserver<RaftProto.RequestVoteResponse> responses) {
        respond("RequestVote", RaftProtoMapper.fromProto(request), RaftMessage.RequestVoteResponse.class, RaftProtoMapper::toProto, responses);
    }

    @Override
    public void preVote(RaftProto.PreVoteRequest request, StreamObserver<RaftProto.PreVoteResponse> responses) {
        respond("PreVote", RaftProtoMapper.fromProto(request), RaftMessage.PreVoteResponse.class, RaftProtoMapper::toProto, responses);
    }

    @Override
    public void appendEntries(RaftProto.AppendEntriesRequest request, StreamObserver<RaftProto.AppendEntriesResponse> responses) {
        respond("AppendEntries", RaftProtoMapper.fromProto(request), RaftMessage.AppendEntriesResponse.class, RaftProtoMapper::toProto, responses);
    }

    @Override
    public void readIndex(RaftProto.ReadIndexRequest request, StreamObserver<RaftProto.ReadIndexResponse> responses) {
        respond("ReadIndex", RaftProtoMapper.fromProto(request), RaftMessage.ReadIndexResponse.class, RaftProtoMapper::toProto, responses);
    }

    @Override
    public StreamObserver<RaftProto.InstallSnapshotRequest> installSnapshot(StreamObserver<RaftProto.InstallSnapshotResponse> responses) {
        String from = sender();
        return new StreamObserver<>() {
            private RaftTransport.SnapshotReceiver receiver;
            private boolean failed;

            @Override
            public void onNext(RaftProto.InstallSnapshotRequest request) {
                if (failed) {
                    return;
                }
                try {
                    switch (request.getPayloadCase()) {
                        case HEADER -> {
                            if (receiver != null) {
                                throw new IllegalArgumentException("Duplicate snapshot header");
                            }
                            receiver = inbound.receiveSnapshot(from, RaftProtoMapper.fromProto(request.getHeader()));
                        }
                        case CHUNK -> {
                            if (receiver == null) {
                                throw new IllegalArgumentException("Snapshot chunk received before header");
                            }
                            receiver.write(request.getChunk().asReadOnlyByteBuffer());
                        }
                        case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Snapshot request payload not set");
                    }
                } catch (IOException | RuntimeException e) {
                    fail(e);
                }
            }

            @Override
            public void onError(Throwable error) {
                if (receiver != null) {
                    receiver.abort();
                }
                log.atDebug().addKeyValue("from", from).addKeyValue("error", error.getMessage()).log("Snapshot stream failed");
            }

            @Override
            public void onCompleted() {
                if (failed) {
                    return;
                }
                if (receiver == null) {
                    fail(new IllegalArgumentException("Snapshot stream ended without a header"));
                    return;
                }
                CompletableFuture<RaftMessage.InstallSnapshotResponse> response;
                try {
                    response = receiver.complete();
                } catch (IOException | RuntimeException e) {
                    fail(e);
                    return;
                }
                response.whenComplete((message, error) -> {
                    if (error != null) {
                        responses.onError(Status.fromThrowable(error).asRuntimeException());
                    } else {
                        responses.onNext(RaftProtoMapper.toProto(message));
                        responses.onCompleted();
                    }
                });
            }

            private void fail(Exception error) {
                failed = true;
                if (receiver != null) {
                    receiver.abort();
                }
                log.atDebug().addKeyValue("from", from).addKeyValue("error", error.getMessage()).log("Snapshot install failed");
                responses.onError(Status.INTERNAL.withDescription(error.getMessage()).withCause(error).asRuntimeException());
            }
        };
    }

    private <M extends RaftMessage.Response, P> void respond(
        String rpc,
        RaftMessage.Request request,
        Class<M> responseType,
        Function<M, P> mapper,
        StreamObserver<P> responses
    ) {
        String from = sender();
        inbound.receive(from, request).whenComplete((response, error) -> {
            if (error != null) {
                log.atDebug().addKeyValue("rpc", rpc).addKeyValue("from", from).addKeyValue("error", error.getMessage()).log("RPC failed");
                responses.onError(Status.fromThrowable(error).asRuntimeException());
            } else {
                responses.onNext(mapper.apply(responseType.cast(response)));
                responses.onCompleted();
            }
        });
    }

    private static String sender() {
        String sender = SENDER.get();
        return sender != null ? sender : "unknown";
    }
}
