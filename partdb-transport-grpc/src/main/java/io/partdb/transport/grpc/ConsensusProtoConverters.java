package io.partdb.transport.grpc;

import com.google.protobuf.ByteString;
import io.partdb.bytes.Bytes;
import io.partdb.consensus.transport.ConsensusRpc;
import io.partdb.transport.grpc.raft.proto.RaftProto;

final class ConsensusProtoConverters {

    static ConsensusRpc.RequestVote fromProto(RaftProto.RequestVoteRequest proto) {
        return new ConsensusRpc.RequestVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.RequestVoteRequest toProto(ConsensusRpc.RequestVote msg) {
        return RaftProto.RequestVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static ConsensusRpc.RequestVoteResponse fromProto(RaftProto.RequestVoteResponse proto) {
        return new ConsensusRpc.RequestVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.RequestVoteResponse toProto(ConsensusRpc.RequestVoteResponse msg) {
        return RaftProto.RequestVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static ConsensusRpc.PreVote fromProto(RaftProto.PreVoteRequest proto) {
        return new ConsensusRpc.PreVote(
            proto.getTerm(),
            proto.getCandidateId(),
            proto.getLastLogIndex(),
            proto.getLastLogTerm()
        );
    }

    static RaftProto.PreVoteRequest toProto(ConsensusRpc.PreVote msg) {
        return RaftProto.PreVoteRequest.newBuilder()
            .setTerm(msg.term())
            .setCandidateId(msg.candidateId())
            .setLastLogIndex(msg.lastLogIndex())
            .setLastLogTerm(msg.lastLogTerm())
            .build();
    }

    static ConsensusRpc.PreVoteResponse fromProto(RaftProto.PreVoteResponse proto) {
        return new ConsensusRpc.PreVoteResponse(
            proto.getTerm(),
            proto.getVoteGranted()
        );
    }

    static RaftProto.PreVoteResponse toProto(ConsensusRpc.PreVoteResponse msg) {
        return RaftProto.PreVoteResponse.newBuilder()
            .setTerm(msg.term())
            .setVoteGranted(msg.voteGranted())
            .build();
    }

    static ConsensusRpc.AppendEntries fromProto(RaftProto.AppendEntriesRequest proto) {
        var entries = proto.getEntriesList().stream()
            .map(ConsensusModelProtoConverters::fromProto)
            .toList();

        return new ConsensusRpc.AppendEntries(
            proto.getTerm(),
            proto.getLeaderId(),
            proto.getPrevLogIndex(),
            proto.getPrevLogTerm(),
            entries,
            proto.getLeaderCommit()
        );
    }

    static RaftProto.AppendEntriesRequest toProto(ConsensusRpc.AppendEntries msg) {
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

    static ConsensusRpc.AppendEntriesResponse fromProto(RaftProto.AppendEntriesResponse proto) {
        return new ConsensusRpc.AppendEntriesResponse(
            proto.getTerm(),
            proto.getSuccess(),
            proto.getMatchIndex()
        );
    }

    static RaftProto.AppendEntriesResponse toProto(ConsensusRpc.AppendEntriesResponse msg) {
        return RaftProto.AppendEntriesResponse.newBuilder()
            .setTerm(msg.term())
            .setSuccess(msg.success())
            .setMatchIndex(msg.matchIndex())
            .build();
    }

    static ConsensusRpc.InstallSnapshotResponse fromProto(RaftProto.InstallSnapshotResponse proto) {
        return new ConsensusRpc.InstallSnapshotResponse(proto.getTerm());
    }

    static RaftProto.InstallSnapshotResponse toProto(ConsensusRpc.InstallSnapshotResponse msg) {
        return RaftProto.InstallSnapshotResponse.newBuilder()
            .setTerm(msg.term())
            .build();
    }

    static ConsensusRpc.ReadIndex fromProto(RaftProto.ReadIndexRequest proto) {
        return new ConsensusRpc.ReadIndex(
            proto.getTerm(),
            Bytes.copyOf(proto.getContext().toByteArray())
        );
    }

    static RaftProto.ReadIndexRequest toProto(ConsensusRpc.ReadIndex msg) {
        return RaftProto.ReadIndexRequest.newBuilder()
            .setTerm(msg.term())
            .setContext(ByteString.copyFrom(msg.context().asReadOnlyByteBuffer()))
            .build();
    }

    static ConsensusRpc.ReadIndexResponse fromProto(RaftProto.ReadIndexResponse proto) {
        return new ConsensusRpc.ReadIndexResponse(
            proto.getTerm(),
            proto.getReadIndex(),
            Bytes.copyOf(proto.getContext().toByteArray())
        );
    }

    static RaftProto.ReadIndexResponse toProto(ConsensusRpc.ReadIndexResponse msg) {
        return RaftProto.ReadIndexResponse.newBuilder()
            .setTerm(msg.term())
            .setReadIndex(msg.readIndex())
            .setContext(ByteString.copyFrom(msg.context().asReadOnlyByteBuffer()))
            .build();
    }

    static RaftProto.SnapshotHeader toSnapshotHeader(ConsensusRpc.InstallSnapshot msg) {
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setLastIncludedIndex(msg.lastIncludedIndex())
            .setLastIncludedTerm(msg.lastIncludedTerm())
            .setMembership(ConsensusModelProtoConverters.toProto(msg.membership()))
            .setTotalSize(msg.data().size())
            .build();
    }

    static ConsensusRpc.InstallSnapshot fromSnapshotHeader(RaftProto.SnapshotHeader header, byte[] data) {
        return new ConsensusRpc.InstallSnapshot(
            header.getTerm(),
            header.getLeaderId(),
            header.getLastIncludedIndex(),
            header.getLastIncludedTerm(),
            ConsensusModelProtoConverters.fromProto(header.getMembership()),
            Bytes.copyOf(data)
        );
    }
}
