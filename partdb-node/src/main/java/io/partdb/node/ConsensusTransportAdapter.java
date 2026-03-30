package io.partdb.node;

import io.partdb.node.transport.ConsensusLogEntry;
import io.partdb.node.transport.ConsensusMessage;
import io.partdb.node.transport.ConsensusTransport;
import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftTransport;

import java.util.concurrent.CompletableFuture;

final class ConsensusTransportAdapter implements RaftTransport {

    private final ConsensusTransport delegate;

    ConsensusTransportAdapter(ConsensusTransport delegate) {
        this.delegate = delegate;
    }

    @Override
    public void start(RpcHandler handler) {
        delegate.start((from, request) -> handler.handle(from, toRaftRequest(request))
            .thenApply(ConsensusTransportAdapter::fromRaftResponse));
    }

    @Override
    public CompletableFuture<RaftMessage.Response> send(String to, RaftMessage.Request request) {
        return delegate.send(to, fromRaftRequest(request))
            .thenApply(ConsensusTransportAdapter::toRaftResponse);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static ConsensusMessage.Request fromRaftRequest(RaftMessage.Request request) {
        return switch (request) {
            case RaftMessage.AppendEntries msg -> new ConsensusMessage.AppendEntries(
                msg.term(),
                msg.leaderId(),
                msg.prevLogIndex(),
                msg.prevLogTerm(),
                msg.entries().stream().map(ConsensusTransportAdapter::fromRaftLogEntry).toList(),
                msg.leaderCommit()
            );
            case RaftMessage.RequestVote msg -> new ConsensusMessage.RequestVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case RaftMessage.InstallSnapshot msg -> new ConsensusMessage.InstallSnapshot(
                msg.term(),
                msg.leaderId(),
                msg.lastIncludedIndex(),
                msg.lastIncludedTerm(),
                NodeMembership.fromRaftMembership(msg.membership()),
                msg.data()
            );
            case RaftMessage.PreVote msg -> new ConsensusMessage.PreVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case RaftMessage.ReadIndex msg -> new ConsensusMessage.ReadIndex(
                msg.term(),
                msg.context()
            );
        };
    }

    private static ConsensusMessage.Response fromRaftResponse(RaftMessage.Response response) {
        return switch (response) {
            case RaftMessage.AppendEntriesResponse msg -> new ConsensusMessage.AppendEntriesResponse(
                msg.term(),
                msg.success(),
                msg.matchIndex()
            );
            case RaftMessage.RequestVoteResponse msg -> new ConsensusMessage.RequestVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case RaftMessage.InstallSnapshotResponse msg -> new ConsensusMessage.InstallSnapshotResponse(
                msg.term()
            );
            case RaftMessage.PreVoteResponse msg -> new ConsensusMessage.PreVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case RaftMessage.ReadIndexResponse msg -> new ConsensusMessage.ReadIndexResponse(
                msg.term(),
                msg.readIndex(),
                msg.context()
            );
        };
    }

    private static RaftMessage.Request toRaftRequest(ConsensusMessage.Request request) {
        return switch (request) {
            case ConsensusMessage.AppendEntries msg -> new RaftMessage.AppendEntries(
                msg.term(),
                msg.leaderId(),
                msg.prevLogIndex(),
                msg.prevLogTerm(),
                msg.entries().stream().map(ConsensusTransportAdapter::toRaftLogEntry).toList(),
                msg.leaderCommit()
            );
            case ConsensusMessage.RequestVote msg -> new RaftMessage.RequestVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ConsensusMessage.InstallSnapshot msg -> new RaftMessage.InstallSnapshot(
                msg.term(),
                msg.leaderId(),
                msg.lastIncludedIndex(),
                msg.lastIncludedTerm(),
                msg.membership().toRaftMembership(),
                msg.data()
            );
            case ConsensusMessage.PreVote msg -> new RaftMessage.PreVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ConsensusMessage.ReadIndex msg -> new RaftMessage.ReadIndex(
                msg.term(),
                msg.context()
            );
        };
    }

    private static RaftMessage.Response toRaftResponse(ConsensusMessage.Response response) {
        return switch (response) {
            case ConsensusMessage.AppendEntriesResponse msg -> new RaftMessage.AppendEntriesResponse(
                msg.term(),
                msg.success(),
                msg.matchIndex()
            );
            case ConsensusMessage.RequestVoteResponse msg -> new RaftMessage.RequestVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ConsensusMessage.InstallSnapshotResponse msg -> new RaftMessage.InstallSnapshotResponse(
                msg.term()
            );
            case ConsensusMessage.PreVoteResponse msg -> new RaftMessage.PreVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ConsensusMessage.ReadIndexResponse msg -> new RaftMessage.ReadIndexResponse(
                msg.term(),
                msg.readIndex(),
                msg.context()
            );
        };
    }

    private static ConsensusLogEntry fromRaftLogEntry(LogEntry entry) {
        return switch (entry) {
            case LogEntry.Data data -> new ConsensusLogEntry.Data(data.index(), data.term(), data.data());
            case LogEntry.NoOp noOp -> new ConsensusLogEntry.NoOp(noOp.index(), noOp.term());
            case LogEntry.Config config -> new ConsensusLogEntry.Config(
                config.index(),
                config.term(),
                NodeMembership.fromRaftMembership(config.membership())
            );
        };
    }

    private static LogEntry toRaftLogEntry(ConsensusLogEntry entry) {
        return switch (entry) {
            case ConsensusLogEntry.Data data -> new LogEntry.Data(data.index(), data.term(), data.data());
            case ConsensusLogEntry.NoOp noOp -> new LogEntry.NoOp(noOp.index(), noOp.term());
            case ConsensusLogEntry.Config config -> new LogEntry.Config(
                config.index(),
                config.term(),
                config.membership().toRaftMembership()
            );
        };
    }
}
