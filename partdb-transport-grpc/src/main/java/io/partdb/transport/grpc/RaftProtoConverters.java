package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftMessage;
import io.partdb.transport.grpc.raft.proto.RaftProto;

final class RaftProtoConverters {

    static RaftMessage.RequestVote fromProto(RaftProto.RequestVoteRequest proto) {
        return new RaftMessage.RequestVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.RequestVoteRequest toProto(RaftMessage.RequestVote msg) {
        return RaftProto.RequestVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static RaftMessage.RequestVoteResponse fromProto(RaftProto.RequestVoteResponse proto) {
        return new RaftMessage.RequestVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.RequestVoteResponse toProto(RaftMessage.RequestVoteResponse msg) {
        return RaftProto.RequestVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static RaftMessage.PreVote fromProto(RaftProto.PreVoteRequest proto) {
        return new RaftMessage.PreVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.PreVoteRequest toProto(RaftMessage.PreVote msg) {
        return RaftProto.PreVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static RaftMessage.PreVoteResponse fromProto(RaftProto.PreVoteResponse proto) {
        return new RaftMessage.PreVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.PreVoteResponse toProto(RaftMessage.PreVoteResponse msg) {
        return RaftProto.PreVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static RaftMessage.AppendEntries fromProto(RaftProto.AppendEntriesRequest proto) {
        var entries = proto.getEntriesList().stream()
            .map(RaftModelProtoConverters::fromProto)
            .toList();

        return new RaftMessage.AppendEntries(
            proto.getTerm(),
            proto.getLeaderId(),
            proto.getPrevLogIndex(),
            proto.getPrevLogTerm(),
            entries,
            proto.getLeaderCommit()
        );
    }

    static RaftProto.AppendEntriesRequest toProto(RaftMessage.AppendEntries msg) {
        var builder = RaftProto.AppendEntriesRequest.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setPrevLogIndex(msg.prevLogIndex())
            .setPrevLogTerm(msg.prevLogTerm())
            .setLeaderCommit(msg.leaderCommit());

        for (var entry : msg.entries()) {
            builder.addEntries(RaftModelProtoConverters.toProto(entry));
        }

        return builder.build();
    }

    static RaftMessage.AppendEntriesResponse fromProto(RaftProto.AppendEntriesResponse proto) {
        return new RaftMessage.AppendEntriesResponse(
            proto.getTerm(),
            proto.getSuccess(),
            proto.getMatchIndex()
        );
    }

    static RaftProto.AppendEntriesResponse toProto(RaftMessage.AppendEntriesResponse msg) {
        return RaftProto.AppendEntriesResponse.newBuilder()
            .setTerm(msg.term())
            .setSuccess(msg.success())
            .setMatchIndex(msg.matchIndex())
            .build();
    }

    static RaftMessage.InstallSnapshotResponse fromProto(RaftProto.InstallSnapshotResponse proto) {
        return new RaftMessage.InstallSnapshotResponse(proto.getTerm());
    }

    static RaftProto.InstallSnapshotResponse toProto(RaftMessage.InstallSnapshotResponse msg) {
        return RaftProto.InstallSnapshotResponse.newBuilder()
            .setTerm(msg.term())
            .build();
    }

    static RaftMessage.ReadIndex fromProto(RaftProto.ReadIndexRequest proto) {
        return new RaftMessage.ReadIndex(
            proto.getTerm(),
            Bytes.copyOf(proto.getContext().toByteArray())
        );
    }

    static RaftProto.ReadIndexRequest toProto(RaftMessage.ReadIndex msg) {
        return RaftProto.ReadIndexRequest.newBuilder()
            .setTerm(msg.term())
            .setContext(ByteString.copyFrom(msg.context().asReadOnlyByteBuffer()))
            .build();
    }

    static RaftMessage.ReadIndexResponse fromProto(RaftProto.ReadIndexResponse proto) {
        return new RaftMessage.ReadIndexResponse(
            proto.getTerm(),
            proto.getReadIndex(),
            Bytes.copyOf(proto.getContext().toByteArray())
        );
    }

    static RaftProto.ReadIndexResponse toProto(RaftMessage.ReadIndexResponse msg) {
        return RaftProto.ReadIndexResponse.newBuilder()
            .setTerm(msg.term())
            .setReadIndex(msg.readIndex())
            .setContext(ByteString.copyFrom(msg.context().asReadOnlyByteBuffer()))
            .build();
    }

    static RaftProto.SnapshotHeader toSnapshotHeader(RaftMessage.InstallSnapshot msg) {
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setLastIncludedIndex(msg.lastIncludedIndex())
            .setLastIncludedTerm(msg.lastIncludedTerm())
            .setMembership(RaftModelProtoConverters.toProto(msg.configuration()))
            .setTotalSize(msg.data().size())
            .build();
    }

    static RaftMessage.InstallSnapshot fromSnapshotHeader(RaftProto.SnapshotHeader header, byte[] data) {
        return new RaftMessage.InstallSnapshot(
            header.getTerm(),
            header.getLeaderId(),
            header.getLastIncludedIndex(),
            header.getLastIncludedTerm(),
            RaftModelProtoConverters.fromProto(header.getMembership()),
            Bytes.copyOf(data)
        );
    }
}
