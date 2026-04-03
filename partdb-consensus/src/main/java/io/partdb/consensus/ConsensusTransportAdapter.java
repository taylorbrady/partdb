package io.partdb.consensus;

import io.partdb.consensus.transport.ConsensusLogEntry;
import io.partdb.consensus.transport.ConsensusRpc;
import io.partdb.consensus.transport.ConsensusTransport;
import io.partdb.consensus.RaftTransport;
import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftMessage;

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

    private static ConsensusRpc.Request fromRaftRequest(RaftMessage.Request request) {
        return switch (request) {
            case RaftMessage.AppendEntries msg -> new ConsensusRpc.AppendEntries(
                msg.term(),
                msg.leaderId(),
                msg.prevLogIndex(),
                msg.prevLogTerm(),
                msg.entries().stream().map(ConsensusTransportAdapter::fromRaftLogEntry).toList(),
                msg.leaderCommit()
            );
            case RaftMessage.RequestVote msg -> new ConsensusRpc.RequestVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case RaftMessage.InstallSnapshot msg -> new ConsensusRpc.InstallSnapshot(
                msg.term(),
                msg.leaderId(),
                msg.lastIncludedIndex(),
                msg.lastIncludedTerm(),
                ClusterMembership.fromRaftMembership(msg.membership()),
                msg.data()
            );
            case RaftMessage.PreVote msg -> new ConsensusRpc.PreVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case RaftMessage.ReadIndex msg -> new ConsensusRpc.ReadIndex(
                msg.term(),
                msg.context()
            );
        };
    }

    private static ConsensusRpc.Response fromRaftResponse(RaftMessage.Response response) {
        return switch (response) {
            case RaftMessage.AppendEntriesResponse msg -> new ConsensusRpc.AppendEntriesResponse(
                msg.term(),
                msg.success(),
                msg.matchIndex()
            );
            case RaftMessage.RequestVoteResponse msg -> new ConsensusRpc.RequestVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case RaftMessage.InstallSnapshotResponse msg -> new ConsensusRpc.InstallSnapshotResponse(
                msg.term()
            );
            case RaftMessage.PreVoteResponse msg -> new ConsensusRpc.PreVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case RaftMessage.ReadIndexResponse msg -> new ConsensusRpc.ReadIndexResponse(
                msg.term(),
                msg.readIndex(),
                msg.context()
            );
        };
    }

    private static RaftMessage.Request toRaftRequest(ConsensusRpc.Request request) {
        return switch (request) {
            case ConsensusRpc.AppendEntries msg -> new RaftMessage.AppendEntries(
                msg.term(),
                msg.leaderId(),
                msg.prevLogIndex(),
                msg.prevLogTerm(),
                msg.entries().stream().map(ConsensusTransportAdapter::toRaftLogEntry).toList(),
                msg.leaderCommit()
            );
            case ConsensusRpc.RequestVote msg -> new RaftMessage.RequestVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ConsensusRpc.InstallSnapshot msg -> new RaftMessage.InstallSnapshot(
                msg.term(),
                msg.leaderId(),
                msg.lastIncludedIndex(),
                msg.lastIncludedTerm(),
                msg.membership().toRaftMembership(),
                msg.data()
            );
            case ConsensusRpc.PreVote msg -> new RaftMessage.PreVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ConsensusRpc.ReadIndex msg -> new RaftMessage.ReadIndex(
                msg.term(),
                msg.context()
            );
        };
    }

    private static RaftMessage.Response toRaftResponse(ConsensusRpc.Response response) {
        return switch (response) {
            case ConsensusRpc.AppendEntriesResponse msg -> new RaftMessage.AppendEntriesResponse(
                msg.term(),
                msg.success(),
                msg.matchIndex()
            );
            case ConsensusRpc.RequestVoteResponse msg -> new RaftMessage.RequestVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ConsensusRpc.InstallSnapshotResponse msg -> new RaftMessage.InstallSnapshotResponse(
                msg.term()
            );
            case ConsensusRpc.PreVoteResponse msg -> new RaftMessage.PreVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ConsensusRpc.ReadIndexResponse msg -> new RaftMessage.ReadIndexResponse(
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
                ClusterMembership.fromRaftMembership(config.membership())
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
