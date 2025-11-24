package io.partdb.raft.transport;

import com.google.protobuf.ByteString;
import io.partdb.common.ByteArray;
import io.partdb.common.statemachine.Delete;
import io.partdb.common.statemachine.GrantLease;
import io.partdb.common.statemachine.KeepAliveLease;
import io.partdb.common.statemachine.Operation;
import io.partdb.common.statemachine.Put;
import io.partdb.common.statemachine.RevokeLease;
import io.partdb.raft.LogEntry;
import io.partdb.raft.rpc.AppendEntriesRequest;
import io.partdb.raft.rpc.AppendEntriesResponse;
import io.partdb.raft.rpc.InstallSnapshotResponse;
import io.partdb.raft.rpc.RequestVoteRequest;
import io.partdb.raft.rpc.RequestVoteResponse;
import io.partdb.raft.rpc.proto.RaftProto;

import java.util.ArrayList;
import java.util.List;

final class ProtobufConverter {

    private ProtobufConverter() {
    }

    static RaftProto.RequestVoteRequest toProto(RequestVoteRequest request) {
        return RaftProto.RequestVoteRequest.newBuilder()
            .setTerm(request.term())
            .setCandidateId(request.candidateId())
            .setLastLogIndex(request.lastLogIndex())
            .setLastLogTerm(request.lastLogTerm())
            .build();
    }

    static RequestVoteRequest fromProto(RaftProto.RequestVoteRequest proto) {
        return new RequestVoteRequest(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.RequestVoteResponse toProto(RequestVoteResponse response) {
        return RaftProto.RequestVoteResponse.newBuilder()
            .setTerm(response.term())
            .setVoteGranted(response.voteGranted())
            .build();
    }

    static RequestVoteResponse fromProto(RaftProto.RequestVoteResponse proto) {
        return new RequestVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.AppendEntriesRequest toProto(AppendEntriesRequest request) {
        List<RaftProto.LogEntry> protoEntries = new ArrayList<>();
        for (LogEntry entry : request.entries()) {
            protoEntries.add(toProtoLogEntry(entry));
        }

        return RaftProto.AppendEntriesRequest.newBuilder()
            .setTerm(request.term())
            .setLeaderId(request.leaderId())
            .setPrevLogIndex(request.prevLogIndex())
            .setPrevLogTerm(request.prevLogTerm())
            .addAllEntries(protoEntries)
            .setLeaderCommit(request.leaderCommit())
            .build();
    }

    static AppendEntriesRequest fromProto(RaftProto.AppendEntriesRequest proto) {
        List<LogEntry> entries = new ArrayList<>();
        long index = proto.getPrevLogIndex();

        for (RaftProto.LogEntry protoEntry : proto.getEntriesList()) {
            index++;
            entries.add(fromProtoLogEntry(protoEntry, index));
        }

        return new AppendEntriesRequest(
            proto.getTerm(),
            proto.getLeaderId(),
            proto.getPrevLogIndex(),
            proto.getPrevLogTerm(),
            entries,
            proto.getLeaderCommit()
        );
    }

    static RaftProto.AppendEntriesResponse toProto(AppendEntriesResponse response) {
        return RaftProto.AppendEntriesResponse.newBuilder()
            .setTerm(response.term())
            .setSuccess(response.success())
            .setMatchIndex(response.matchIndex())
            .build();
    }

    static AppendEntriesResponse fromProto(RaftProto.AppendEntriesResponse proto) {
        return new AppendEntriesResponse(
            proto.getTerm(),
            proto.getSuccess(),
            proto.getMatchIndex()
        );
    }

    static RaftProto.InstallSnapshotResponse toProto(InstallSnapshotResponse response) {
        return RaftProto.InstallSnapshotResponse.newBuilder()
            .setTerm(response.term())
            .build();
    }

    static InstallSnapshotResponse fromProto(RaftProto.InstallSnapshotResponse proto) {
        return new InstallSnapshotResponse(proto.getTerm());
    }

    private static RaftProto.LogEntry toProtoLogEntry(LogEntry entry) {
        return RaftProto.LogEntry.newBuilder()
            .setTerm(entry.term())
            .setOperation(toProtoOperation(entry.command()))
            .build();
    }

    private static LogEntry fromProtoLogEntry(RaftProto.LogEntry proto, long index) {
        return new LogEntry(
            index,
            proto.getTerm(),
            fromProtoOperation(proto.getOperation())
        );
    }

    private static RaftProto.Operation toProtoOperation(Operation operation) {
        return switch (operation) {
            case Put put -> RaftProto.Operation.newBuilder()
                .setPut(RaftProto.Put.newBuilder()
                    .setKey(toByteString(put.key()))
                    .setValue(toByteString(put.value()))
                    .setLeaseId(put.leaseId())
                    .build())
                .build();
            case Delete delete -> RaftProto.Operation.newBuilder()
                .setDelete(RaftProto.Delete.newBuilder()
                    .setKey(toByteString(delete.key()))
                    .build())
                .build();
            case GrantLease grantLease -> RaftProto.Operation.newBuilder()
                .setGrantLease(RaftProto.GrantLease.newBuilder()
                    .setLeaseId(grantLease.leaseId())
                    .setTtlMillis(grantLease.ttlMillis())
                    .setGrantedAtMillis(grantLease.grantedAtMillis())
                    .build())
                .build();
            case RevokeLease revokeLease -> RaftProto.Operation.newBuilder()
                .setRevokeLease(RaftProto.RevokeLease.newBuilder()
                    .setLeaseId(revokeLease.leaseId())
                    .build())
                .build();
            case KeepAliveLease keepAliveLease -> RaftProto.Operation.newBuilder()
                .setKeepAliveLease(RaftProto.KeepAliveLease.newBuilder()
                    .setLeaseId(keepAliveLease.leaseId())
                    .build())
                .build();
        };
    }

    private static Operation fromProtoOperation(RaftProto.Operation proto) {
        return switch (proto.getOpCase()) {
            case PUT -> {
                RaftProto.Put put = proto.getPut();
                yield new Put(
                    fromByteString(put.getKey()),
                    fromByteString(put.getValue()),
                    put.getLeaseId()
                );
            }
            case DELETE -> {
                RaftProto.Delete delete = proto.getDelete();
                yield new Delete(fromByteString(delete.getKey()));
            }
            case GRANT_LEASE -> {
                RaftProto.GrantLease grantLease = proto.getGrantLease();
                yield new GrantLease(
                    grantLease.getLeaseId(),
                    grantLease.getTtlMillis(),
                    grantLease.getGrantedAtMillis()
                );
            }
            case REVOKE_LEASE -> {
                RaftProto.RevokeLease revokeLease = proto.getRevokeLease();
                yield new RevokeLease(revokeLease.getLeaseId());
            }
            case KEEP_ALIVE_LEASE -> {
                RaftProto.KeepAliveLease keepAliveLease = proto.getKeepAliveLease();
                yield new KeepAliveLease(keepAliveLease.getLeaseId());
            }
            case OP_NOT_SET -> throw new IllegalArgumentException("Operation not set");
        };
    }

    private static ByteString toByteString(ByteArray byteArray) {
        return ByteString.copyFrom(byteArray.toByteArray());
    }

    private static ByteArray fromByteString(ByteString byteString) {
        return ByteArray.wrap(byteString.toByteArray());
    }
}
