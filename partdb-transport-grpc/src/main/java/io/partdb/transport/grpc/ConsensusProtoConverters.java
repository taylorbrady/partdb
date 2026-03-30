package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.node.transport.ConsensusMessage;
import io.partdb.transport.grpc.raft.proto.RaftProto;

final class ConsensusProtoConverters {

    static ConsensusMessage.RequestVote fromProto(RaftProto.RequestVoteRequest proto) {
        return new ConsensusMessage.RequestVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.RequestVoteRequest toProto(ConsensusMessage.RequestVote msg) {
        return RaftProto.RequestVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static ConsensusMessage.RequestVoteResponse fromProto(RaftProto.RequestVoteResponse proto) {
        return new ConsensusMessage.RequestVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.RequestVoteResponse toProto(ConsensusMessage.RequestVoteResponse msg) {
        return RaftProto.RequestVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static ConsensusMessage.PreVote fromProto(RaftProto.PreVoteRequest proto) {
        return new ConsensusMessage.PreVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.PreVoteRequest toProto(ConsensusMessage.PreVote msg) {
        return RaftProto.PreVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static ConsensusMessage.PreVoteResponse fromProto(RaftProto.PreVoteResponse proto) {
        return new ConsensusMessage.PreVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.PreVoteResponse toProto(ConsensusMessage.PreVoteResponse msg) {
        return RaftProto.PreVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static ConsensusMessage.AppendEntries fromProto(RaftProto.AppendEntriesRequest proto) {
        var entries = proto.getEntriesList().stream()
            .map(ConsensusModelProtoConverters::fromProto)
            .toList();

        return new ConsensusMessage.AppendEntries(
            proto.getTerm(),
            proto.getLeaderId(),
            proto.getPrevLogIndex(),
            proto.getPrevLogTerm(),
            entries,
            proto.getLeaderCommit()
        );
    }

    static RaftProto.AppendEntriesRequest toProto(ConsensusMessage.AppendEntries msg) {
        var builder = RaftProto.AppendEntriesRequest.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setPrevLogIndex(msg.prevLogIndex())
            .setPrevLogTerm(msg.prevLogTerm())
            .setLeaderCommit(msg.leaderCommit());

        for (var entry : msg.entries()) {
            builder.addEntries(ConsensusModelProtoConverters.toProto(entry));
        }

        return builder.build();
    }

    static ConsensusMessage.AppendEntriesResponse fromProto(RaftProto.AppendEntriesResponse proto) {
        return new ConsensusMessage.AppendEntriesResponse(
            proto.getTerm(),
            proto.getSuccess(),
            proto.getMatchIndex()
        );
    }

    static RaftProto.AppendEntriesResponse toProto(ConsensusMessage.AppendEntriesResponse msg) {
        return RaftProto.AppendEntriesResponse.newBuilder()
            .setTerm(msg.term())
            .setSuccess(msg.success())
            .setMatchIndex(msg.matchIndex())
            .build();
    }

    static ConsensusMessage.InstallSnapshotResponse fromProto(RaftProto.InstallSnapshotResponse proto) {
        return new ConsensusMessage.InstallSnapshotResponse(proto.getTerm());
    }

    static RaftProto.InstallSnapshotResponse toProto(ConsensusMessage.InstallSnapshotResponse msg) {
        return RaftProto.InstallSnapshotResponse.newBuilder()
            .setTerm(msg.term())
            .build();
    }

    static ConsensusMessage.ReadIndex fromProto(RaftProto.ReadIndexRequest proto) {
        return new ConsensusMessage.ReadIndex(
            proto.getTerm(),
            proto.getContext().toByteArray()
        );
    }

    static RaftProto.ReadIndexRequest toProto(ConsensusMessage.ReadIndex msg) {
        return RaftProto.ReadIndexRequest.newBuilder()
            .setTerm(msg.term())
            .setContext(ByteString.copyFrom(msg.context()))
            .build();
    }

    static ConsensusMessage.ReadIndexResponse fromProto(RaftProto.ReadIndexResponse proto) {
        return new ConsensusMessage.ReadIndexResponse(
            proto.getTerm(),
            proto.getReadIndex(),
            proto.getContext().toByteArray()
        );
    }

    static RaftProto.ReadIndexResponse toProto(ConsensusMessage.ReadIndexResponse msg) {
        return RaftProto.ReadIndexResponse.newBuilder()
            .setTerm(msg.term())
            .setReadIndex(msg.readIndex())
            .setContext(ByteString.copyFrom(msg.context()))
            .build();
    }

    static RaftProto.SnapshotHeader toSnapshotHeader(ConsensusMessage.InstallSnapshot msg) {
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setLastIncludedIndex(msg.lastIncludedIndex())
            .setLastIncludedTerm(msg.lastIncludedTerm())
            .setMembership(ConsensusModelProtoConverters.toProto(msg.membership()))
            .setTotalSize(msg.data().length)
            .build();
    }

    static ConsensusMessage.InstallSnapshot fromSnapshotHeader(RaftProto.SnapshotHeader header, byte[] data) {
        return new ConsensusMessage.InstallSnapshot(
            header.getTerm(),
            header.getLeaderId(),
            header.getLastIncludedIndex(),
            header.getLastIncludedTerm(),
            ConsensusModelProtoConverters.fromProto(header.getMembership()),
            data
        );
    }
}
