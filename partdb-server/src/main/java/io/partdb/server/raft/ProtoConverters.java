package io.partdb.server.raft;

import com.google.protobuf.ByteString;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Membership;
import io.partdb.raft.RaftMessage;
import io.partdb.server.raft.proto.RaftProto;

import java.util.HashSet;
import java.util.List;

final class ProtoConverters {

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
        List<LogEntry> entries = proto.getEntriesList().stream()
            .map(ProtoConverters::fromProto)
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

        for (LogEntry entry : msg.entries()) {
            builder.addEntries(toProto(entry));
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
            proto.getContext().toByteArray()
        );
    }

    static RaftProto.ReadIndexRequest toProto(RaftMessage.ReadIndex msg) {
        return RaftProto.ReadIndexRequest.newBuilder()
            .setTerm(msg.term())
            .setContext(ByteString.copyFrom(msg.context()))
            .build();
    }

    static RaftMessage.ReadIndexResponse fromProto(RaftProto.ReadIndexResponse proto) {
        return new RaftMessage.ReadIndexResponse(
            proto.getTerm(),
            proto.getReadIndex(),
            proto.getContext().toByteArray()
        );
    }

    static RaftProto.ReadIndexResponse toProto(RaftMessage.ReadIndexResponse msg) {
        return RaftProto.ReadIndexResponse.newBuilder()
            .setTerm(msg.term())
            .setReadIndex(msg.readIndex())
            .setContext(ByteString.copyFrom(msg.context()))
            .build();
    }

    static LogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case DATA -> new LogEntry.Data(
                proto.getIndex(),
                proto.getTerm(),
                proto.getData().toByteArray()
            );
            case NO_OP -> new LogEntry.NoOp(
                proto.getIndex(),
                proto.getTerm()
            );
            case CONFIG -> new LogEntry.Config(
                proto.getIndex(),
                proto.getTerm(),
                fromProto(proto.getConfig())
            );
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("LogEntry type not set");
        };
    }

    static RaftProto.LogEntry toProto(LogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder()
            .setIndex(entry.index())
            .setTerm(entry.term());

        switch (entry) {
            case LogEntry.Data data -> builder.setData(ByteString.copyFrom(data.data()));
            case LogEntry.NoOp _ -> builder.setNoOp(true);
            case LogEntry.Config config -> builder.setConfig(toProto(config.membership()));
        }

        return builder.build();
    }

    static Membership fromProto(RaftProto.Membership proto) {
        return new Membership(
            new HashSet<>(proto.getVotersList()),
            new HashSet<>(proto.getLearnersList())
        );
    }

    static RaftProto.Membership toProto(Membership membership) {
        return RaftProto.Membership.newBuilder()
            .addAllVoters(membership.voters())
            .addAllLearners(membership.learners())
            .build();
    }

    static RaftProto.SnapshotHeader toSnapshotHeader(RaftMessage.InstallSnapshot msg) {
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(msg.term())
            .setLeaderId(msg.leaderId())
            .setLastIncludedIndex(msg.lastIncludedIndex())
            .setLastIncludedTerm(msg.lastIncludedTerm())
            .setMembership(toProto(msg.membership()))
            .setTotalSize(msg.data().length)
            .build();
    }

    static RaftMessage.InstallSnapshot fromSnapshotHeader(RaftProto.SnapshotHeader header, byte[] data) {
        return new RaftMessage.InstallSnapshot(
            header.getTerm(),
            header.getLeaderId(),
            header.getLastIncludedIndex(),
            header.getLastIncludedTerm(),
            fromProto(header.getMembership()),
            data
        );
    }
}
