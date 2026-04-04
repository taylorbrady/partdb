package io.partdb.node.internal.replication;

import io.partdb.cluster.ClusterMembership;
import io.partdb.consensus.transport.ConsensusLogEntry;
import io.partdb.consensus.transport.ConsensusRpc;
import io.partdb.consensus.transport.ConsensusTransport;
import io.partdb.node.replication.ReplicationLogEntry;
import io.partdb.node.replication.ReplicationRpc;
import io.partdb.node.replication.ReplicationTransport;

import java.util.concurrent.CompletableFuture;

public final class ConsensusTransportAdapter implements ConsensusTransport {
    private final ReplicationTransport delegate;

    public ConsensusTransportAdapter(ReplicationTransport delegate) {
        this.delegate = delegate;
    }

    @Override
    public void start(RpcHandler handler) {
        delegate.start((from, request) -> handler.handle(from, toConsensus(request)).thenApply(this::fromConsensus));
    }

    @Override
    public CompletableFuture<ConsensusRpc.Response> send(String to, ConsensusRpc.Request request) {
        return delegate.send(to, fromConsensus(request)).thenApply(ConsensusTransportAdapter::toConsensus);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private static ConsensusRpc.Request toConsensus(ReplicationRpc.Request request) {
        return switch (request) {
            case ReplicationRpc.RequestVote msg -> new ConsensusRpc.RequestVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ReplicationRpc.PreVote msg -> new ConsensusRpc.PreVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ReplicationRpc.AppendEntries msg -> new ConsensusRpc.AppendEntries(
                msg.term(),
                msg.leaderId(),
                msg.prevLogIndex(),
                msg.prevLogTerm(),
                msg.entries().stream().map(ConsensusTransportAdapter::toConsensus).toList(),
                msg.leaderCommit()
            );
            case ReplicationRpc.InstallSnapshot msg -> new ConsensusRpc.InstallSnapshot(
                msg.term(),
                msg.leaderId(),
                msg.lastIncludedIndex(),
                msg.lastIncludedTerm(),
                msg.membership(),
                msg.data()
            );
            case ReplicationRpc.ReadIndex msg -> new ConsensusRpc.ReadIndex(msg.term(), msg.context());
        };
    }

    private static ConsensusRpc.Response toConsensus(ReplicationRpc.Response response) {
        return switch (response) {
            case ReplicationRpc.RequestVoteResponse msg -> new ConsensusRpc.RequestVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ReplicationRpc.PreVoteResponse msg -> new ConsensusRpc.PreVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ReplicationRpc.AppendEntriesResponse msg -> new ConsensusRpc.AppendEntriesResponse(
                msg.term(),
                msg.success(),
                msg.matchIndex()
            );
            case ReplicationRpc.InstallSnapshotResponse msg -> new ConsensusRpc.InstallSnapshotResponse(msg.term());
            case ReplicationRpc.ReadIndexResponse msg -> new ConsensusRpc.ReadIndexResponse(
                msg.term(),
                msg.readIndex(),
                msg.context()
            );
        };
    }

    private ReplicationRpc.Response fromConsensus(ConsensusRpc.Response response) {
        return switch (response) {
            case ConsensusRpc.RequestVoteResponse msg -> new ReplicationRpc.RequestVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ConsensusRpc.PreVoteResponse msg -> new ReplicationRpc.PreVoteResponse(
                msg.term(),
                msg.voteGranted()
            );
            case ConsensusRpc.AppendEntriesResponse msg -> new ReplicationRpc.AppendEntriesResponse(
                msg.term(),
                msg.success(),
                msg.matchIndex()
            );
            case ConsensusRpc.InstallSnapshotResponse msg -> new ReplicationRpc.InstallSnapshotResponse(msg.term());
            case ConsensusRpc.ReadIndexResponse msg -> new ReplicationRpc.ReadIndexResponse(
                msg.term(),
                msg.readIndex(),
                msg.context()
            );
        };
    }

    private static ReplicationRpc.Request fromConsensus(ConsensusRpc.Request request) {
        return switch (request) {
            case ConsensusRpc.RequestVote msg -> new ReplicationRpc.RequestVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ConsensusRpc.PreVote msg -> new ReplicationRpc.PreVote(
                msg.term(),
                msg.candidateId(),
                msg.lastLogIndex(),
                msg.lastLogTerm()
            );
            case ConsensusRpc.AppendEntries msg -> new ReplicationRpc.AppendEntries(
                msg.term(),
                msg.leaderId(),
                msg.prevLogIndex(),
                msg.prevLogTerm(),
                msg.entries().stream().map(ConsensusTransportAdapter::fromConsensus).toList(),
                msg.leaderCommit()
            );
            case ConsensusRpc.InstallSnapshot msg -> new ReplicationRpc.InstallSnapshot(
                msg.term(),
                msg.leaderId(),
                msg.lastIncludedIndex(),
                msg.lastIncludedTerm(),
                msg.membership(),
                msg.data()
            );
            case ConsensusRpc.ReadIndex msg -> new ReplicationRpc.ReadIndex(msg.term(), msg.context());
        };
    }

    private static ConsensusLogEntry toConsensus(ReplicationLogEntry entry) {
        return switch (entry) {
            case ReplicationLogEntry.Data data -> new ConsensusLogEntry.Data(data.index(), data.term(), data.data());
            case ReplicationLogEntry.NoOp noOp -> new ConsensusLogEntry.NoOp(noOp.index(), noOp.term());
            case ReplicationLogEntry.Config config ->
                new ConsensusLogEntry.Config(config.index(), config.term(), config.membership());
        };
    }

    private static ReplicationLogEntry fromConsensus(ConsensusLogEntry entry) {
        return switch (entry) {
            case ConsensusLogEntry.Data data -> new ReplicationLogEntry.Data(data.index(), data.term(), data.data());
            case ConsensusLogEntry.NoOp noOp -> new ReplicationLogEntry.NoOp(noOp.index(), noOp.term());
            case ConsensusLogEntry.Config config ->
                new ReplicationLogEntry.Config(config.index(), config.term(), config.membership());
        };
    }
}
