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

    static RaftProto.RaftMessage toProto(RaftMessage message) {
        var builder = RaftProto.RaftMessage.newBuilder();
        switch (message) {
            case RaftMessage.PreVote vote -> builder.setPreVote(RaftProto.PreVote.newBuilder()
                .setTerm(vote.term()).setLastLogIndex(vote.lastLogIndex()).setLastLogTerm(vote.lastLogTerm()));
            case RaftMessage.PreVoteResponse response -> builder.setPreVoteResponse(RaftProto.PreVoteResponse.newBuilder()
                .setTerm(response.term()).setGranted(response.granted()));
            case RaftMessage.RequestVote vote -> builder.setRequestVote(RaftProto.RequestVote.newBuilder()
                .setTerm(vote.term()).setLastLogIndex(vote.lastLogIndex()).setLastLogTerm(vote.lastLogTerm())
                .setLeadershipTransfer(vote.leadershipTransfer()));
            case RaftMessage.RequestVoteResponse response -> builder.setRequestVoteResponse(RaftProto.RequestVoteResponse.newBuilder()
                .setTerm(response.term()).setGranted(response.granted()));
            case RaftMessage.AppendEntries append -> builder.setAppendEntries(RaftProto.AppendEntries.newBuilder()
                .setTerm(append.term()).setPrevLogIndex(append.prevLogIndex()).setPrevLogTerm(append.prevLogTerm())
                .addAllEntries(append.entries().stream().map(RaftProtoMapper::toProto).toList())
                .setCommitIndex(append.commitIndex()));
            case RaftMessage.AppendAccepted accepted -> builder.setAppendAccepted(RaftProto.AppendAccepted.newBuilder()
                .setTerm(accepted.term()).setMatchIndex(accepted.matchIndex()));
            case RaftMessage.AppendRejected rejected -> builder.setAppendRejected(RaftProto.AppendRejected.newBuilder()
                .setTerm(rejected.term()).setRejectedIndex(rejected.rejectedIndex())
                .setHintIndex(rejected.hintIndex()).setHintTerm(rejected.hintTerm()));
            case RaftMessage.Heartbeat heartbeat -> builder.setHeartbeat(RaftProto.Heartbeat.newBuilder()
                .setTerm(heartbeat.term()).setCommitIndex(heartbeat.commitIndex()).setRound(heartbeat.round()));
            case RaftMessage.HeartbeatResponse response -> builder.setHeartbeatResponse(RaftProto.HeartbeatResponse.newBuilder()
                .setTerm(response.term()).setRound(response.round()));
            case RaftMessage.InstallSnapshot _ -> throw new IllegalArgumentException("snapshots are streamed separately");
            case RaftMessage.InstallSnapshotResponse response -> builder.setInstallSnapshotResult(RaftProto.InstallSnapshotResult.newBuilder()
                .setTerm(response.term()).setMatchIndex(response.matchIndex()));
            case RaftMessage.ReadIndex read -> builder.setReadIndex(RaftProto.ReadIndex.newBuilder()
                .setTerm(read.term()).setId(read.id()));
            case RaftMessage.ReadIndexResponse response -> builder.setReadIndexResponse(RaftProto.ReadIndexResponse.newBuilder()
                .setTerm(response.term()).setId(response.id()).setReadIndex(response.readIndex()));
            case RaftMessage.TimeoutNow timeout -> builder.setTimeoutNow(RaftProto.TimeoutNow.newBuilder().setTerm(timeout.term()));
        }
        return builder.build();
    }

    static RaftMessage fromProto(RaftProto.RaftMessage proto) {
        return switch (proto.getMessageCase()) {
            case PRE_VOTE -> {
                var vote = proto.getPreVote();
                yield new RaftMessage.PreVote(vote.getTerm(), vote.getLastLogIndex(), vote.getLastLogTerm());
            }
            case PRE_VOTE_RESPONSE -> {
                var response = proto.getPreVoteResponse();
                yield new RaftMessage.PreVoteResponse(response.getTerm(), response.getGranted());
            }
            case REQUEST_VOTE -> {
                var vote = proto.getRequestVote();
                yield new RaftMessage.RequestVote(vote.getTerm(), vote.getLastLogIndex(), vote.getLastLogTerm(), vote.getLeadershipTransfer());
            }
            case REQUEST_VOTE_RESPONSE -> {
                var response = proto.getRequestVoteResponse();
                yield new RaftMessage.RequestVoteResponse(response.getTerm(), response.getGranted());
            }
            case APPEND_ENTRIES -> {
                var append = proto.getAppendEntries();
                yield new RaftMessage.AppendEntries(
                    append.getTerm(),
                    append.getPrevLogIndex(),
                    append.getPrevLogTerm(),
                    append.getEntriesList().stream().map(RaftProtoMapper::fromProto).toList(),
                    append.getCommitIndex()
                );
            }
            case APPEND_ACCEPTED -> {
                var accepted = proto.getAppendAccepted();
                yield new RaftMessage.AppendAccepted(accepted.getTerm(), accepted.getMatchIndex());
            }
            case APPEND_REJECTED -> {
                var rejected = proto.getAppendRejected();
                yield new RaftMessage.AppendRejected(rejected.getTerm(), rejected.getRejectedIndex(), rejected.getHintIndex(), rejected.getHintTerm());
            }
            case HEARTBEAT -> {
                var heartbeat = proto.getHeartbeat();
                yield new RaftMessage.Heartbeat(heartbeat.getTerm(), heartbeat.getCommitIndex(), heartbeat.getRound());
            }
            case HEARTBEAT_RESPONSE -> {
                var response = proto.getHeartbeatResponse();
                yield new RaftMessage.HeartbeatResponse(response.getTerm(), response.getRound());
            }
            case INSTALL_SNAPSHOT_RESULT -> {
                var result = proto.getInstallSnapshotResult();
                yield new RaftMessage.InstallSnapshotResponse(result.getTerm(), result.getMatchIndex());
            }
            case READ_INDEX -> {
                var read = proto.getReadIndex();
                yield new RaftMessage.ReadIndex(read.getTerm(), read.getId());
            }
            case READ_INDEX_RESPONSE -> {
                var response = proto.getReadIndexResponse();
                yield new RaftMessage.ReadIndexResponse(response.getTerm(), response.getId(), response.getReadIndex());
            }
            case TIMEOUT_NOW -> new RaftMessage.TimeoutNow(proto.getTimeoutNow().getTerm());
            case MESSAGE_NOT_SET -> throw new IllegalArgumentException("raft message not set");
        };
    }

    static RaftProto.SnapshotHeader toProto(RaftMessage.InstallSnapshot install) {
        var snapshot = install.snapshot();
        return RaftProto.SnapshotHeader.newBuilder()
            .setTerm(install.term())
            .setSnapshotIndex(snapshot.index())
            .setSnapshotTerm(snapshot.term())
            .setMembership(toProto(snapshot.membership()))
            .build();
    }

    static RaftMessage.InstallSnapshot fromProto(RaftProto.SnapshotHeader proto) {
        return new RaftMessage.InstallSnapshot(
            proto.getTerm(),
            new SnapshotMetadata(proto.getSnapshotIndex(), proto.getSnapshotTerm(), fromProto(proto.getMembership()))
        );
    }

    private static RaftProto.LogEntry toProto(RaftLogEntry entry) {
        var builder = RaftProto.LogEntry.newBuilder().setIndex(entry.index()).setTerm(entry.term());
        switch (entry) {
            case RaftLogEntry.Command command -> builder.setCommand(UnsafeByteOperations.unsafeWrap(command.command().asReadOnlyByteBuffer()));
            case RaftLogEntry.NoOp _ -> builder.setNoOp(true);
            case RaftLogEntry.Membership membership -> builder.setMembership(toProto(membership.membership()));
        }
        return builder.build();
    }

    private static RaftLogEntry fromProto(RaftProto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case COMMAND -> new RaftLogEntry.Command(proto.getIndex(), proto.getTerm(), Bytes.copyOf(proto.getCommand().asReadOnlyByteBuffer()));
            case NO_OP -> new RaftLogEntry.NoOp(proto.getIndex(), proto.getTerm());
            case MEMBERSHIP -> new RaftLogEntry.Membership(proto.getIndex(), proto.getTerm(), fromProto(proto.getMembership()));
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("raft log entry not set");
        };
    }

    private static RaftProto.Membership toProto(RaftMembership membership) {
        return RaftProto.Membership.newBuilder()
            .addAllVoters(membership.voters())
            .addAllLearners(membership.learners())
            .build();
    }

    private static RaftMembership fromProto(RaftProto.Membership proto) {
        return RaftMembership.of(Set.copyOf(proto.getVotersList()), Set.copyOf(proto.getLearnersList()));
    }
}
