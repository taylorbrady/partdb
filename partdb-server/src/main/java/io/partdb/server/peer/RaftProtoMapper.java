package io.partdb.server.peer;

import com.google.protobuf.UnsafeByteOperations;
import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.SnapshotMetadata;
import io.partdb.server.peer.proto.RaftProto;

import java.util.Set;

final class RaftProtoMapper {

    private RaftProtoMapper() {
    }

    static RaftProto.RequestVoteRequest toProto(RaftMessage.RequestVote message) {
        return RaftProto.RequestVoteRequest.newBuilder()
            .setTerm(message.term())
            .setCandidateId(message.candidateId())
            .setLastLogIndex(message.lastLogIndex())
            .setLastLogTerm(message.lastLogTerm())
            .build();
    }

    static RaftMessage.RequestVote fromProto(RaftProto.RequestVoteRequest proto) {
        return new RaftMessage.RequestVote(proto.getTerm(), proto.getCandidateId(), proto.getLastLogIndex(), proto.getLastLogTerm());
    }

    static RaftProto.RequestVoteResponse toProto(RaftMessage.RequestVoteResponse message) {
        return RaftProto.RequestVoteResponse.newBuilder()
            .setTerm(message.term())
            .setVoteGranted(message.voteGranted())
            .build();
    }

    static RaftMessage.RequestVoteResponse fromProto(RaftProto.RequestVoteResponse proto) {
        return new RaftMessage.RequestVoteResponse(proto.getTerm(), proto.getVoteGranted());
    }

    static RaftProto.PreVoteRequest toProto(RaftMessage.PreVote message) {
        return RaftProto.PreVoteRequest.newBuilder()
            .setTerm(message.term())
            .setCandidateId(message.candidateId())
            .setLastLogIndex(message.lastLogIndex())
            .setLastLogTerm(message.lastLogTerm())
            .build();
    }

    static RaftMessage.PreVote fromProto(RaftProto.PreVoteRequest proto) {
        return new RaftMessage.PreVote(proto.getTerm(), proto.getCandidateId(), proto.getLastLogIndex(), proto.getLastLogTerm());
    }

    static RaftProto.PreVoteResponse toProto(RaftMessage.PreVoteResponse message) {
        return RaftProto.PreVoteResponse.newBuilder()
            .setTerm(message.term())
            .setVoteGranted(message.voteGranted())
            .build();
    }

    static RaftMessage.PreVoteResponse fromProto(RaftProto.PreVoteResponse proto) {
        return new RaftMessage.PreVoteResponse(proto.getTerm(), proto.getVoteGranted());
    }

    static RaftProto.AppendEntriesRequest toProto(RaftMessage.AppendEntries message) {
        return RaftProto.AppendEntriesRequest.newBuilder()
            .setTerm(message.term())
            .setLeaderId(message.leaderId())
            .setPrevLogIndex(message.prevLogIndex())
            .setPrevLogTerm(message.prevLogTerm())
            .addAllEntries(message.entries().stream().map(RaftProtoMapper::toProto).toList())
            .setLeaderCommit(message.leaderCommit())
            .build();
    }

    static RaftMessage.AppendEntries fromProto(RaftProto.AppendEntriesRequest proto) {
        return new RaftMessage.AppendEntries(
            proto.getTerm(),
            proto.getLeaderId(),
            proto.getPrevLogIndex(),
            proto.getPrevLogTerm(),
            proto.getEntriesList().stream().map(RaftProtoMapper::fromProto).toList(),
            proto.getLeaderCommit()
        );
    }

    static RaftProto.AppendEntriesResponse toProto(RaftMessage.AppendEntriesResponse message) {
        return RaftProto.AppendEntriesResponse.newBuilder()
            .setTerm(message.term())
            .setSuccess(message.success())
            .setMatchIndex(message.matchIndex())
            .build();
    }

    static RaftMessage.AppendEntriesResponse fromProto(RaftProto.AppendEntriesResponse proto) {
        return new RaftMessage.AppendEntriesResponse(proto.getTerm(), proto.getSuccess(), proto.getMatchIndex());
    }

    static RaftProto.SnapshotHeader toProto(RaftMessage.InstallSnapshot message) {
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(message.term())
            .setLeaderId(message.leaderId())
            .setSnapshotIndex(message.snapshot().index())
            .setSnapshotTerm(message.snapshot().term())
            .setMembership(toProto(message.snapshot().membership()))
            .build();
    }

    static RaftMessage.InstallSnapshot fromProto(RaftProto.SnapshotHeader proto) {
        return new RaftMessage.InstallSnapshot(
            proto.getTerm(),
            proto.getLeaderId(),
            new SnapshotMetadata(proto.getSnapshotIndex(), proto.getSnapshotTerm(), fromProto(proto.getMembership()))
        );
    }

    static RaftProto.InstallSnapshotResponse toProto(RaftMessage.InstallSnapshotResponse message) {
        return RaftProto.InstallSnapshotResponse.newBuilder().setTerm(message.term()).build();
    }

    static RaftMessage.InstallSnapshotResponse fromProto(RaftProto.InstallSnapshotResponse proto) {
        return new RaftMessage.InstallSnapshotResponse(proto.getTerm());
    }

    static RaftProto.ReadIndexRequest toProto(RaftMessage.ReadIndexRequested message) {
        return RaftProto.ReadIndexRequest.newBuilder()
            .setTerm(message.term())
            .setContext(message.context())
            .build();
    }

    static RaftMessage.ReadIndexRequested fromProto(RaftProto.ReadIndexRequest proto) {
        return new RaftMessage.ReadIndexRequested(proto.getTerm(), proto.getContext());
    }

    static RaftProto.ReadIndexResponse toProto(RaftMessage.ReadIndexResponse message) {
        return RaftProto.ReadIndexResponse.newBuilder()
            .setTerm(message.term())
            .setReadIndex(message.readIndex())
            .setContext(message.context())
            .build();
    }

    static RaftMessage.ReadIndexResponse fromProto(RaftProto.ReadIndexResponse proto) {
        return new RaftMessage.ReadIndexResponse(proto.getTerm(), proto.getReadIndex(), proto.getContext());
    }

    private static RaftProto.LogEntry toProto(RaftLogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder()
            .setIndex(entry.index())
            .setTerm(entry.term());
        switch (entry) {
            case RaftLogEntry.Data data -> builder.setData(UnsafeByteOperations.unsafeWrap(data.data().asReadOnlyByteBuffer()));
            case RaftLogEntry.NoOp _ -> builder.setNoOp(true);
            case RaftLogEntry.Config config -> builder.setConfig(toProto(config.membership()));
        }
        return builder.build();
    }

    private static RaftLogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case DATA -> new RaftLogEntry.Data(proto.getIndex(), proto.getTerm(), Bytes.copyOf(proto.getData().asReadOnlyByteBuffer()));
            case NO_OP -> new RaftLogEntry.NoOp(proto.getIndex(), proto.getTerm());
            case CONFIG -> new RaftLogEntry.Config(proto.getIndex(), proto.getTerm(), fromProto(proto.getConfig()));
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("Raft log entry type not set");
        };
    }

    private static RaftProto.Membership toProto(RaftMembership membership) {
        return RaftProto.Membership.newBuilder()
            .addAllVoters(membership.voters())
            .addAllLearners(membership.learners())
            .build();
    }

    private static RaftMembership fromProto(RaftProto.Membership proto) {
        return new RaftMembership(Set.copyOf(proto.getVotersList()), Set.copyOf(proto.getLearnersList()));
    }
}
