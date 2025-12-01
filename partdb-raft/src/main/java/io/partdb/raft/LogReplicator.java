package io.partdb.raft;

import io.partdb.raft.rpc.AppendEntriesRequest;
import io.partdb.raft.rpc.AppendEntriesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public final class LogReplicator {
    private static final Logger logger = LoggerFactory.getLogger(LogReplicator.class);

    private final String nodeId;
    private final ClusterConfig cluster;
    private final RaftLog log;
    private final RaftTransport transport;
    private final RaftConfig config;
    private final ExecutorService executor;
    private final Map<String, PeerReplicationState> peerStates;

    public LogReplicator(
        String nodeId,
        ClusterConfig cluster,
        RaftLog log,
        RaftTransport transport,
        RaftConfig config,
        ExecutorService executor
    ) {
        this.nodeId = nodeId;
        this.cluster = cluster;
        this.log = log;
        this.transport = transport;
        this.config = config;
        this.executor = executor;
        this.peerStates = new ConcurrentHashMap<>();

        for (String peer : cluster.peerNodeIds()) {
            peerStates.put(peer, new PeerReplicationState());
        }
    }

    public void resetPeerStates(long lastLogIndex) {
        for (String peer : peerStates.keySet()) {
            peerStates.put(peer, new PeerReplicationState(lastLogIndex + 1, 0));
        }
    }

    public void replicateToAll(long currentTerm, long commitIndex, java.util.function.LongConsumer onHigherTerm) {
        for (String peerId : peerStates.keySet()) {
            executor.submit(() -> replicateToPeer(peerId, currentTerm, commitIndex, onHigherTerm));
        }
    }

    private void replicateToPeer(String peerId, long currentTerm, long commitIndex, java.util.function.LongConsumer onHigherTerm) {
        PeerReplicationState state = peerStates.get(peerId);
        if (state == null) {
            return;
        }

        long logFirstIndex = log.firstIndex();
        if (state.nextIndex() < logFirstIndex) {
            return;
        }

        long prevLogIndex = state.nextIndex() - 1;
        long prevLogTerm = prevLogIndex > 0
            ? log.get(prevLogIndex).map(LogEntry::term).orElse(0L)
            : 0;

        long endIndex = Math.min(
            state.nextIndex() + config.maxEntriesPerAppend(),
            log.lastIndex() + 1
        );
        List<LogEntry> entries = new ArrayList<>(log.getRange(state.nextIndex(), endIndex));

        AppendEntriesRequest request = new AppendEntriesRequest(
            currentTerm,
            nodeId,
            prevLogIndex,
            prevLogTerm,
            entries,
            commitIndex
        );

        transport.appendEntries(peerId, request)
            .thenAccept(response -> handleAppendEntriesResponse(
                peerId, response, currentTerm, onHigherTerm
            ))
            .exceptionally(ex -> {
                logger.warn("Failed to replicate to peer {}: {}", peerId, ex.getMessage());
                return null;
            });
    }

    private void handleAppendEntriesResponse(
        String peerId,
        AppendEntriesResponse response,
        long requestTerm,
        java.util.function.LongConsumer onHigherTerm
    ) {
        if (response.term() > requestTerm) {
            onHigherTerm.accept(response.term());
            return;
        }

        peerStates.computeIfPresent(peerId, (id, state) -> {
            if (response.success()) {
                return state.withMatch(response.matchIndex());
            } else {
                return state.decrementNext();
            }
        });
    }

    public long calculateCommitIndex(long currentTerm, long currentCommitIndex) {
        for (long n = log.lastIndex(); n > currentCommitIndex; n--) {
            if (log.get(n).map(entry -> entry.term() != currentTerm).orElse(true)) {
                continue;
            }

            int replicationCount = 1;
            for (PeerReplicationState state : peerStates.values()) {
                if (state.matchIndex() >= n) {
                    replicationCount++;
                }
            }

            if (replicationCount >= cluster.quorum()) {
                return n;
            }
        }

        return currentCommitIndex;
    }

    public PeerReplicationState getPeerState(String peerId) {
        return peerStates.get(peerId);
    }

    public Map<String, PeerReplicationState> getAllPeerStates() {
        return Map.copyOf(peerStates);
    }
}
