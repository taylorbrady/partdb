package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.node.replication.ReplicationRpc;
import io.partdb.transport.grpc.raft.proto.RaftProto;

final class ReplicationProtoConverters {

    static ReplicationRpc.RequestVote fromProto(RaftProto.RequestVoteRequest proto) {
        return new ReplicationRpc.RequestVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.RequestVoteRequest toProto(ReplicationRpc.RequestVote msg) {
        return RaftProto.RequestVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static ReplicationRpc.RequestVoteResponse fromProto(RaftProto.RequestVoteResponse proto) {
        return new ReplicationRpc.RequestVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.RequestVoteResponse toProto(ReplicationRpc.RequestVoteResponse msg) {
        return RaftProto.RequestVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static ReplicationRpc.PreVote fromProto(RaftProto.PreVoteRequest proto) {
        return new ReplicationRpc.PreVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.PreVoteRequest toProto(ReplicationRpc.PreVote msg) {
        return RaftProto.PreVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static ReplicationRpc.PreVoteResponse fromProto(RaftProto.PreVoteResponse proto) {
        return new ReplicationRpc.PreVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.PreVoteResponse toProto(ReplicationRpc.PreVoteResponse msg) {
        return RaftProto.PreVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static ReplicationRpc.AppendEntries fromProto(RaftProto.AppendEntriesRequest proto) {
        var entries = proto.getEntriesList().stream()
            .map(ReplicationModelProtoConverters::fromProto)
            .toList();

        return new ReplicationRpc.AppendEntries(
            proto.getTerm(),
            proto.getLeaderId(),
            proto.getPrevLogIndex(),
            proto.getPrevLogTerm(),
            entries,
            proto.getLeaderCommit()
        );
    }

    static RaftProto.AppendEntriesRequest toProto(ReplicationRpc.AppendEntries msg) {
        var builder = RaftProto.AppendEntriesRequest.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setPrevLogIndex(msg.prevLogIndex())
            .setPrevLogTerm(msg.prevLogTerm())
            .setLeaderCommit(msg.leaderCommit());

        for (var entry : msg.entries()) {
            builder.addEntries(ReplicationModelProtoConverters.toProto(entry));
        }

        return builder.build();
    }

    static ReplicationRpc.AppendEntriesResponse fromProto(RaftProto.AppendEntriesResponse proto) {
        return new ReplicationRpc.AppendEntriesResponse(
            proto.getTerm(),
            proto.getSuccess(),
            proto.getMatchIndex()
        );
    }

    static RaftProto.AppendEntriesResponse toProto(ReplicationRpc.AppendEntriesResponse msg) {
        return RaftProto.AppendEntriesResponse.newBuilder()
            .setTerm(msg.term())
            .setSuccess(msg.success())
            .setMatchIndex(msg.matchIndex())
            .build();
    }

    static ReplicationRpc.InstallSnapshotResponse fromProto(RaftProto.InstallSnapshotResponse proto) {
        return new ReplicationRpc.InstallSnapshotResponse(proto.getTerm());
    }

    static RaftProto.InstallSnapshotResponse toProto(ReplicationRpc.InstallSnapshotResponse msg) {
        return RaftProto.InstallSnapshotResponse.newBuilder()
            .setTerm(msg.term())
            .build();
    }

    static ReplicationRpc.ReadIndex fromProto(RaftProto.ReadIndexRequest proto) {
        return new ReplicationRpc.ReadIndex(
            proto.getTerm(),
            Bytes.copyOf(proto.getContext().toByteArray())
        );
    }

    static RaftProto.ReadIndexRequest toProto(ReplicationRpc.ReadIndex msg) {
        return RaftProto.ReadIndexRequest.newBuilder()
            .setTerm(msg.term())
            .setContext(ByteString.copyFrom(msg.context().asReadOnlyByteBuffer()))
            .build();
    }

    static ReplicationRpc.ReadIndexResponse fromProto(RaftProto.ReadIndexResponse proto) {
        return new ReplicationRpc.ReadIndexResponse(
            proto.getTerm(),
            proto.getReadIndex(),
            Bytes.copyOf(proto.getContext().toByteArray())
        );
    }

    static RaftProto.ReadIndexResponse toProto(ReplicationRpc.ReadIndexResponse msg) {
        return RaftProto.ReadIndexResponse.newBuilder()
            .setTerm(msg.term())
            .setReadIndex(msg.readIndex())
            .setContext(ByteString.copyFrom(msg.context().asReadOnlyByteBuffer()))
            .build();
    }

    static RaftProto.SnapshotHeader toSnapshotHeader(ReplicationRpc.InstallSnapshot msg) {
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setLastIncludedIndex(msg.lastIncludedIndex())
            .setLastIncludedTerm(msg.lastIncludedTerm())
            .setMembership(ReplicationModelProtoConverters.toProto(msg.membership()))
            .setTotalSize(msg.data().size())
            .build();
    }

    static ReplicationRpc.InstallSnapshot fromSnapshotHeader(RaftProto.SnapshotHeader header, byte[] data) {
        return new ReplicationRpc.InstallSnapshot(
            header.getTerm(),
            header.getLeaderId(),
            header.getLastIncludedIndex(),
            header.getLastIncludedTerm(),
            ReplicationModelProtoConverters.fromProto(header.getMembership()),
            Bytes.copyOf(data)
        );
    }
}
