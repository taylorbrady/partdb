package io.partdb.raft;

import io.partdb.common.ClusterConfig;
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
    private final Map<String, Progress> peerProgress;

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
        this.peerProgress = new ConcurrentHashMap<>();

        for (String peer : cluster.peerNodeIds()) {
            peerProgress.put(peer, new Progress());
        }
    }

    public void resetProgress(long lastLogIndex) {
        peerProgress.replaceAll((_, _) -> new Progress(lastLogIndex + 1, 0));
    }

    public void replicateToAll(long currentTerm, long commitIndex, java.util.function.LongConsumer onHigherTerm) {
        for (String peerId : peerProgress.keySet()) {
            executor.submit(() -> replicateToPeer(peerId, currentTerm, commitIndex, onHigherTerm));
        }
    }

    private void replicateToPeer(String peerId, long currentTerm, long commitIndex, java.util.function.LongConsumer onHigherTerm) {
        Progress progress = peerProgress.get(peerId);
        if (progress == null) {
            return;
        }

        long logFirstIndex = log.firstIndex();
        if (progress.nextIndex() < logFirstIndex) {
            return;
        }

        long prevLogIndex = progress.nextIndex() - 1;
        long prevLogTerm = prevLogIndex > 0
            ? log.get(prevLogIndex).map(LogEntry::term).orElse(0L)
            : 0;

        long endIndex = Math.min(
            progress.nextIndex() + config.maxEntriesPerAppend(),
            log.lastIndex() + 1
        );
        List<LogEntry> entries = new ArrayList<>(log.getRange(progress.nextIndex(), endIndex));

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

        peerProgress.computeIfPresent(peerId, (_, progress) -> {
            if (response.success()) {
                return progress.withMatch(response.matchIndex());
            } else {
                return progress.decrementNext();
            }
        });
    }

    public long calculateCommitIndex(long currentTerm, long currentCommitIndex) {
        for (long n = log.lastIndex(); n > currentCommitIndex; n--) {
            if (log.get(n).map(entry -> entry.term() != currentTerm).orElse(true)) {
                continue;
            }

            int replicationCount = 1;
            for (Progress progress : peerProgress.values()) {
                if (progress.matchIndex() >= n) {
                    replicationCount++;
                }
            }

            if (replicationCount >= cluster.quorum()) {
                return n;
            }
        }

        return currentCommitIndex;
    }

    public Progress getProgress(String peerId) {
        return peerProgress.get(peerId);
    }
}
