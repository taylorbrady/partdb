package io.partdb.server.peer;

import com.google.protobuf.UnsafeByteOperations;
import io.partdb.bytes.Bytes;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Membership;
import io.partdb.raft.Message;
import io.partdb.raft.NodeId;
import io.partdb.raft.SnapshotMetadata;
import io.partdb.raft.runtime.SnapshotInfo;
import io.partdb.server.peer.proto.AppendAccepted;
import io.partdb.server.peer.proto.AppendEntries;
import io.partdb.server.peer.proto.AppendRejected;
import io.partdb.server.peer.proto.Heartbeat;
import io.partdb.server.peer.proto.HeartbeatResponse;
import io.partdb.server.peer.proto.NoOp;
import io.partdb.server.peer.proto.PeerMessage;
import io.partdb.server.peer.proto.PreVote;
import io.partdb.server.peer.proto.PreVoteResponse;
import io.partdb.server.peer.proto.ReadIndexRequest;
import io.partdb.server.peer.proto.ReadIndexResponse;
import io.partdb.server.peer.proto.RequestVote;
import io.partdb.server.peer.proto.RequestVoteResponse;
import io.partdb.server.peer.proto.SnapshotHeader;
import io.partdb.server.peer.proto.SnapshotInstalled;
import io.partdb.server.peer.proto.TimeoutNow;

import java.util.OptionalLong;
import java.util.stream.Collectors;

final class RaftProtoMapper {

    private RaftProtoMapper() {
    }

    static PeerMessage toProto(Message message) {
        var builder = PeerMessage.newBuilder();
        switch (message) {
            case Message.PreVote vote -> builder.setPreVote(PreVote.newBuilder()
                .setTerm(vote.term()).setLastLogIndex(vote.lastLogIndex()).setLastLogTerm(vote.lastLogTerm()));
            case Message.PreVoteResponse response -> builder.setPreVoteResponse(PreVoteResponse.newBuilder()
                .setTerm(response.term()).setGranted(response.granted()));
            case Message.RequestVote vote -> builder.setRequestVote(RequestVote.newBuilder()
                .setTerm(vote.term()).setLastLogIndex(vote.lastLogIndex()).setLastLogTerm(vote.lastLogTerm())
                .setLeadershipTransfer(vote.leadershipTransfer()));
            case Message.RequestVoteResponse response -> builder.setRequestVoteResponse(RequestVoteResponse.newBuilder()
                .setTerm(response.term()).setGranted(response.granted()));
            case Message.AppendEntries append -> builder.setAppendEntries(AppendEntries.newBuilder()
                .setTerm(append.term()).setPrevLogIndex(append.prevLogIndex()).setPrevLogTerm(append.prevLogTerm())
                .addAllEntries(append.entries().stream().map(RaftProtoMapper::toProto).toList())
                .setCommitIndex(append.commitIndex()));
            case Message.AppendAccepted accepted -> builder.setAppendAccepted(AppendAccepted.newBuilder()
                .setTerm(accepted.term()).setMatchIndex(accepted.matchIndex()));
            case Message.AppendRejected rejected -> builder.setAppendRejected(AppendRejected.newBuilder()
                .setTerm(rejected.term()).setRejectedIndex(rejected.rejectedIndex())
                .setHintIndex(rejected.hintIndex()).setHintTerm(rejected.hintTerm()));
            case Message.Heartbeat heartbeat -> builder.setHeartbeat(Heartbeat.newBuilder()
                .setTerm(heartbeat.term()).setCommitIndex(heartbeat.commitIndex()).setRound(heartbeat.round()));
            case Message.HeartbeatResponse response -> builder.setHeartbeatResponse(HeartbeatResponse.newBuilder()
                .setTerm(response.term()).setRound(response.round()));
            case Message.SnapshotInstalled installed -> builder.setSnapshotInstalled(SnapshotInstalled.newBuilder()
                .setTerm(installed.term()).setMatchIndex(installed.matchIndex()));
            case Message.ReadIndexRequest read -> builder.setReadIndexRequest(ReadIndexRequest.newBuilder()
                .setTerm(read.term()).setId(read.id()));
            case Message.ReadIndexResponse response -> {
                var proto = ReadIndexResponse.newBuilder().setTerm(response.term()).setId(response.id());
                response.readIndex().ifPresent(proto::setReadIndex);
                builder.setReadIndexResponse(proto);
            }
            case Message.TimeoutNow timeout -> builder.setTimeoutNow(TimeoutNow.newBuilder().setTerm(timeout.term()));
        }
        return builder.build();
    }

    static Message fromProto(PeerMessage proto) {
        return switch (proto.getMessageCase()) {
            case PRE_VOTE -> {
                var vote = proto.getPreVote();
                yield new Message.PreVote(vote.getTerm(), vote.getLastLogIndex(), vote.getLastLogTerm());
            }
            case PRE_VOTE_RESPONSE -> {
                var response = proto.getPreVoteResponse();
                yield new Message.PreVoteResponse(response.getTerm(), response.getGranted());
            }
            case REQUEST_VOTE -> {
                var vote = proto.getRequestVote();
                yield new Message.RequestVote(vote.getTerm(), vote.getLastLogIndex(), vote.getLastLogTerm(), vote.getLeadershipTransfer());
            }
            case REQUEST_VOTE_RESPONSE -> {
                var response = proto.getRequestVoteResponse();
                yield new Message.RequestVoteResponse(response.getTerm(), response.getGranted());
            }
            case APPEND_ENTRIES -> {
                var append = proto.getAppendEntries();
                yield new Message.AppendEntries(
                    append.getTerm(),
                    append.getPrevLogIndex(),
                    append.getPrevLogTerm(),
                    append.getEntriesList().stream().map(RaftProtoMapper::fromProto).toList(),
                    append.getCommitIndex()
                );
            }
            case APPEND_ACCEPTED -> {
                var accepted = proto.getAppendAccepted();
                yield new Message.AppendAccepted(accepted.getTerm(), accepted.getMatchIndex());
            }
            case APPEND_REJECTED -> {
                var rejected = proto.getAppendRejected();
                yield new Message.AppendRejected(rejected.getTerm(), rejected.getRejectedIndex(), rejected.getHintIndex(), rejected.getHintTerm());
            }
            case HEARTBEAT -> {
                var heartbeat = proto.getHeartbeat();
                yield new Message.Heartbeat(heartbeat.getTerm(), heartbeat.getCommitIndex(), heartbeat.getRound());
            }
            case HEARTBEAT_RESPONSE -> {
                var response = proto.getHeartbeatResponse();
                yield new Message.HeartbeatResponse(response.getTerm(), response.getRound());
            }
            case SNAPSHOT_INSTALLED -> {
                var installed = proto.getSnapshotInstalled();
                yield new Message.SnapshotInstalled(installed.getTerm(), installed.getMatchIndex());
            }
            case READ_INDEX_REQUEST -> {
                var read = proto.getReadIndexRequest();
                yield new Message.ReadIndexRequest(read.getTerm(), read.getId());
            }
            case READ_INDEX_RESPONSE -> {
                var response = proto.getReadIndexResponse();
                yield new Message.ReadIndexResponse(
                    response.getTerm(),
                    response.getId(),
                    response.hasReadIndex() ? OptionalLong.of(response.getReadIndex()) : OptionalLong.empty()
                );
            }
            case TIMEOUT_NOW -> new Message.TimeoutNow(proto.getTimeoutNow().getTerm());
            case MESSAGE_NOT_SET -> throw new IllegalArgumentException("peer message not set");
        };
    }

    static SnapshotHeader toProto(long term, SnapshotInfo info) {
        var snapshot = info.metadata();
        return SnapshotHeader.newBuilder()
            .setTerm(term)
            .setSnapshotIndex(snapshot.index())
            .setSnapshotTerm(snapshot.term())
            .setMembership(toProto(snapshot.membership()))
            .setSizeBytes(info.size())
            .setCrc32C(info.checksum())
            .build();
    }

    static SnapshotInfo fromProto(SnapshotHeader proto) {
        var metadata = new SnapshotMetadata(proto.getSnapshotIndex(), proto.getSnapshotTerm(), fromProto(proto.getMembership()));
        return new SnapshotInfo(metadata, proto.getSizeBytes(), proto.getCrc32C());
    }

    private static io.partdb.server.peer.proto.LogEntry toProto(LogEntry entry) {
        var builder = io.partdb.server.peer.proto.LogEntry.newBuilder().setIndex(entry.index()).setTerm(entry.term());
        switch (entry) {
            case LogEntry.Command command -> builder.setCommand(UnsafeByteOperations.unsafeWrap(command.command().asReadOnlyByteBuffer()));
            case LogEntry.NoOp _ -> builder.setNoOp(NoOp.getDefaultInstance());
            case LogEntry.Configuration configuration -> builder.setConfiguration(toProto(configuration.membership()));
        }
        return builder.build();
    }

    private static LogEntry fromProto(io.partdb.server.peer.proto.LogEntry proto) {
        return switch (proto.getEntryCase()) {
            case COMMAND -> new LogEntry.Command(proto.getIndex(), proto.getTerm(), Bytes.copyOf(proto.getCommand().asReadOnlyByteBuffer()));
            case NO_OP -> new LogEntry.NoOp(proto.getIndex(), proto.getTerm());
            case CONFIGURATION -> new LogEntry.Configuration(proto.getIndex(), proto.getTerm(), fromProto(proto.getConfiguration()));
            case ENTRY_NOT_SET -> throw new IllegalArgumentException("log entry not set");
        };
    }

    private static io.partdb.server.peer.proto.Membership toProto(Membership membership) {
        return io.partdb.server.peer.proto.Membership.newBuilder()
            .addAllVoters(membership.voters().stream().map(NodeId::value).toList())
            .addAllLearners(membership.learners().stream().map(NodeId::value).toList())
            .build();
    }

    private static Membership fromProto(io.partdb.server.peer.proto.Membership proto) {
        return Membership.of(
            proto.getVotersList().stream().map(NodeId::new).collect(Collectors.toSet()),
            proto.getLearnersList().stream().map(NodeId::new).collect(Collectors.toSet())
        );
    }
}
