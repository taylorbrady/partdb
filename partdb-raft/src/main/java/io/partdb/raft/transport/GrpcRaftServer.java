package io.partdb.raft.transport;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import io.partdb.raft.RaftNode;
import io.partdb.raft.rpc.InstallSnapshotRequest;
import io.partdb.raft.rpc.proto.RaftProto;
import io.partdb.raft.rpc.proto.RaftServiceGrpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32C;

public final class GrpcRaftServer extends RaftServiceGrpc.RaftServiceImplBase implements AutoCloseable {

    private final RaftNode raftNode;
    private final RaftTransportConfig config;
    private final Server server;

    public GrpcRaftServer(RaftNode raftNode, RaftTransportConfig config) {
        this.raftNode = raftNode;
        this.config = config;

        ServerBuilder<?> builder = ServerBuilder.forPort(config.bindPort())
            .addService(this);

        this.server = builder.build();
    }

    public void start() throws IOException {
        server.start();
    }

    @Override
    public void requestVote(
        RaftProto.RequestVoteRequest request,
        StreamObserver<RaftProto.RequestVoteResponse> responseObserver
    ) {
        try {
            var javaRequest = ProtobufConverter.fromProto(request);
            var javaResponse = raftNode.handleRequestVote(javaRequest);
            var protoResponse = ProtobufConverter.toProto(javaResponse);

            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public void appendEntries(
        RaftProto.AppendEntriesRequest request,
        StreamObserver<RaftProto.AppendEntriesResponse> responseObserver
    ) {
        try {
            var javaRequest = ProtobufConverter.fromProto(request);
            var javaResponse = raftNode.handleAppendEntries(javaRequest);
            var protoResponse = ProtobufConverter.toProto(javaResponse);

            responseObserver.onNext(protoResponse);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(e);
        }
    }

    @Override
    public StreamObserver<RaftProto.SnapshotChunk> installSnapshot(
        StreamObserver<RaftProto.InstallSnapshotResponse> responseObserver
    ) {
        return new StreamObserver<>() {
            private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            private String snapshotId;
            private long term;
            private String leaderId;
            private long lastIncludedIndex;
            private long lastIncludedTerm;

            @Override
            public void onNext(RaftProto.SnapshotChunk chunk) {
                try {
                    if (snapshotId == null) {
                        snapshotId = chunk.getSnapshotId();
                        term = chunk.getTerm();
                        leaderId = chunk.getLeaderId();
                        lastIncludedIndex = chunk.getLastIncludedIndex();
                        lastIncludedTerm = chunk.getLastIncludedTerm();
                    } else if (!snapshotId.equals(chunk.getSnapshotId())) {
                        responseObserver.onError(new IllegalArgumentException("Snapshot ID mismatch"));
                        return;
                    }

                    buffer.write(chunk.getData().toByteArray());

                    if (chunk.getDone()) {
                        byte[] snapshotData = buffer.toByteArray();

                        var crc = new CRC32C();
                        crc.update(snapshotData);

                        InstallSnapshotRequest request = new InstallSnapshotRequest(
                            term,
                            leaderId,
                            lastIncludedIndex,
                            lastIncludedTerm,
                            snapshotData,
                            crc.getValue()
                        );

                        var javaResponse = raftNode.handleInstallSnapshot(request);
                        var protoResponse = ProtobufConverter.toProto(javaResponse);

                        responseObserver.onNext(protoResponse);
                        responseObserver.onCompleted();
                    }
                } catch (Exception e) {
                    responseObserver.onError(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                try {
                    buffer.close();
                } catch (IOException e) {
                }
            }

            @Override
            public void onCompleted() {
                try {
                    buffer.close();
                } catch (IOException e) {
                }
            }
        };
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow();
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
