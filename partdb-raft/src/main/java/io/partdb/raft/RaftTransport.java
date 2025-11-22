package io.partdb.raft;

import io.partdb.raft.rpc.AppendEntriesRequest;
import io.partdb.raft.rpc.AppendEntriesResponse;
import io.partdb.raft.rpc.InstallSnapshotRequest;
import io.partdb.raft.rpc.InstallSnapshotResponse;
import io.partdb.raft.rpc.RequestVoteRequest;
import io.partdb.raft.rpc.RequestVoteResponse;

import java.util.concurrent.CompletableFuture;

public interface RaftTransport {
    CompletableFuture<RequestVoteResponse> requestVote(String nodeId, RequestVoteRequest request);

    CompletableFuture<AppendEntriesResponse> appendEntries(String nodeId, AppendEntriesRequest request);

    CompletableFuture<InstallSnapshotResponse> installSnapshot(String nodeId, InstallSnapshotRequest request);
}
