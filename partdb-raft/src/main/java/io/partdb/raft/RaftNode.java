package io.partdb.raft;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;
import io.partdb.common.exception.NotLeaderException;
import io.partdb.common.exception.TooManyRequestsException;
import io.partdb.common.statemachine.Operation;
import io.partdb.common.statemachine.StateMachine;
import io.partdb.common.statemachine.StateSnapshot;
import io.partdb.raft.rpc.AppendEntriesRequest;
import io.partdb.raft.rpc.AppendEntriesResponse;
import io.partdb.raft.rpc.InstallSnapshotRequest;
import io.partdb.raft.rpc.InstallSnapshotResponse;
import io.partdb.raft.rpc.RequestVoteRequest;
import io.partdb.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

public final class RaftNode implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final RaftConfig config;
    private final StateMachine stateMachine;
    private final RaftLog log;
    private final RaftTransport transport;
    private final SnapshotManager snapshotManager;
    private final RaftMetadataStore metadataStore;

    private final Object stateLock = new Object();
    private final AtomicReference<RaftNodeState> state;

    private final LogReplicator logReplicator;
    private final ElectionTimer electionTimer;
    private final HeartbeatScheduler heartbeatScheduler;

    private final BlockingQueue<ProposalRequest> proposalQueue;
    private final Map<Long, CompletableFuture<Void>> pendingProposals;
    private final Map<Long, List<CompletableFuture<Void>>> applyWaiters;
    private final Object commitNotifier;

    private final ScheduledExecutorService scheduler;
    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService executor;

    private final AtomicBoolean snapshotInProgress;
    private volatile Instant lastSnapshotTime;
    private final Duration snapshotCheckInterval;

    private volatile boolean closed;

    public RaftNode(
        RaftConfig config,
        StateMachine stateMachine,
        RaftTransport transport
    ) {
        this.nodeId = config.nodeId();
        this.config = config;
        this.stateMachine = stateMachine;
        this.transport = transport;

        this.log = SegmentedRaftLog.open(
            new RaftLogConfig(config.dataDirectory().resolve("raft-log"), RaftLogConfig.DEFAULT_SEGMENT_SIZE)
        );

        this.snapshotManager = new SnapshotManager(
            config.dataDirectory().resolve("snapshots"),
            stateMachine,
            log
        );

        try {
            this.metadataStore = RaftMetadataStore.open(config.dataDirectory());
            RaftMetadata metadata = metadataStore.load();
            this.state = new AtomicReference<>(new RaftNodeState(
                metadata.currentTerm(),
                NodeRole.FOLLOWER,
                metadata.votedFor(),
                null,
                0,
                0
            ));
        } catch (Exception e) {
            throw new RaftException.MetadataException("Failed to initialize metadata store", e);
        }

        this.proposalQueue = new LinkedBlockingQueue<>(config.maxProposalQueueSize());
        this.pendingProposals = new ConcurrentHashMap<>();
        this.applyWaiters = new ConcurrentHashMap<>();
        this.commitNotifier = new Object();

        this.scheduler = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
        this.snapshotScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("raft-snapshot-", 0).factory()
        );
        this.executor = Executors.newVirtualThreadPerTaskExecutor();

        this.logReplicator = new LogReplicator(
            nodeId,
            config.cluster(),
            log,
            transport,
            config,
            executor
        );

        long electionTimeoutBase = config.electionTimeoutMinMs();
        long electionTimeoutJitter = config.electionTimeoutMaxMs() - config.electionTimeoutMinMs();
        this.electionTimer = new ElectionTimer(scheduler, electionTimeoutBase, electionTimeoutJitter);

        this.heartbeatScheduler = new HeartbeatScheduler(scheduler, config.heartbeatIntervalMs());

        this.snapshotInProgress = new AtomicBoolean(false);
        this.lastSnapshotTime = Instant.MIN;
        this.snapshotCheckInterval = Duration.ofSeconds(10);

        this.closed = false;

        Optional<RaftSnapshot> snapshot = snapshotManager.loadLatestSnapshot();
        if (snapshot.isPresent()) {
            stateMachine.restore(snapshot.get().stateSnapshot());
            state.updateAndGet(s -> s
                .withLastApplied(snapshot.get().lastIncludedIndex())
                .withCommitIndex(snapshot.get().lastIncludedIndex())
            );
        }

        electionTimer.start(this::startElection);
        startApplyThread();
        startSnapshotMonitor();
    }

    public CompletableFuture<Void> propose(Operation operation) {
        RaftNodeState currentState = state.get();
        if (!currentState.isLeader()) {
            return CompletableFuture.failedFuture(
                new NotLeaderException(Optional.ofNullable(currentState.leaderId()))
            );
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        ProposalRequest request = new ProposalRequest(operation, future);

        if (!proposalQueue.offer(request)) {
            return CompletableFuture.failedFuture(
                new TooManyRequestsException(proposalQueue.size())
            );
        }

        processProposals();

        return future;
    }

    public boolean isLeader() {
        return state.get().isLeader();
    }

    public CompletableFuture<Optional<ByteArray>> get(ByteArray key) {
        RaftNodeState currentState = state.get();
        if (!currentState.isLeader()) {
            return CompletableFuture.failedFuture(
                new NotLeaderException(Optional.ofNullable(currentState.leaderId()))
            );
        }

        return confirmLeadership()
            .thenCompose(this::waitForApplied)
            .thenApply(v -> stateMachine.get(key));
    }

    public CompletableFuture<Iterator<Entry>> scan(ByteArray startKey, ByteArray endKey) {
        RaftNodeState currentState = state.get();
        if (!currentState.isLeader()) {
            return CompletableFuture.failedFuture(
                new NotLeaderException(Optional.ofNullable(currentState.leaderId()))
            );
        }

        return confirmLeadership()
            .thenCompose(this::waitForApplied)
            .thenApply(v -> stateMachine.scan(startKey, endKey));
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        RaftNodeState currentState = state.get();

        if (request.term() > currentState.currentTerm()) {
            stepDown(request.term());
            currentState = state.get();
        }

        if (request.term() < currentState.currentTerm()) {
            return new RequestVoteResponse(currentState.currentTerm(), false);
        }

        if (currentState.votedFor() != null && !currentState.votedFor().equals(request.candidateId())) {
            return new RequestVoteResponse(currentState.currentTerm(), false);
        }

        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();

        boolean logUpToDate = request.lastLogTerm() > lastLogTerm ||
            (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);

        if (logUpToDate) {
            transitionState(s -> s.withVote(request.candidateId()));
            electionTimer.reset(this::startElection);
            return new RequestVoteResponse(state.get().currentTerm(), true);
        }

        return new RequestVoteResponse(state.get().currentTerm(), false);
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        RaftNodeState currentState = state.get();

        if (request.term() > currentState.currentTerm()) {
            stepDown(request.term());
            currentState = state.get();
        }

        if (request.term() < currentState.currentTerm()) {
            return new AppendEntriesResponse(currentState.currentTerm(), false, 0);
        }

        state.updateAndGet(s -> s.withLeader(request.leaderId()));
        electionTimer.reset(this::startElection);

        if (currentState.isCandidate()) {
            transitionState(s -> s.becomeFollower(s.currentTerm()));
        }

        if (request.prevLogIndex() > 0) {
            Optional<LogEntry> prevEntry = log.get(request.prevLogIndex());

            if (prevEntry.isEmpty() || prevEntry.get().term() != request.prevLogTerm()) {
                return new AppendEntriesResponse(state.get().currentTerm(), false, 0);
            }
        }

        long matchIndex = request.prevLogIndex();

        for (LogEntry entry : request.entries()) {
            Optional<LogEntry> existing = log.get(entry.index());

            if (existing.isPresent() && existing.get().term() != entry.term()) {
                log.truncateAfter(entry.index() - 1);
            }

            if (log.get(entry.index()).isEmpty()) {
                log.append(entry);
            }

            matchIndex = entry.index();
        }

        if (request.leaderCommit() > currentState.commitIndex()) {
            long newCommitIndex = Math.min(request.leaderCommit(), matchIndex);
            state.updateAndGet(s -> s.withCommitIndex(newCommitIndex));
            synchronized (commitNotifier) {
                commitNotifier.notifyAll();
            }
        }

        return new AppendEntriesResponse(state.get().currentTerm(), true, matchIndex);
    }

    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        RaftNodeState currentState = state.get();

        if (request.term() > currentState.currentTerm()) {
            stepDown(request.term());
            currentState = state.get();
        }

        if (request.term() < currentState.currentTerm()) {
            return new InstallSnapshotResponse(currentState.currentTerm());
        }

        state.updateAndGet(s -> s.withLeader(request.leaderId()));
        electionTimer.reset(this::startElection);

        var stateSnapshot = StateSnapshot.restore(
            request.lastIncludedIndex(),
            request.data(),
            request.checksum()
        );

        stateSnapshot.verify();

        RaftSnapshot snapshot = new RaftSnapshot(
            request.lastIncludedIndex(),
            request.lastIncludedTerm(),
            stateSnapshot
        );

        snapshotManager.installSnapshot(snapshot);

        state.updateAndGet(s -> s
            .withCommitIndex(request.lastIncludedIndex())
            .withLastApplied(request.lastIncludedIndex())
        );

        return new InstallSnapshotResponse(state.get().currentTerm());
    }

    @Override
    public void close() {
        closed = true;

        electionTimer.cancel();
        heartbeatScheduler.cancel();

        scheduler.shutdown();
        snapshotScheduler.shutdown();
        executor.shutdown();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!snapshotScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                snapshotScheduler.shutdownNow();
            }
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            snapshotScheduler.shutdownNow();
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.close();
        snapshotManager.close();
        metadataStore.close();
    }

    private void transitionState(UnaryOperator<RaftNodeState> transition) {
        synchronized (stateLock) {
            state.updateAndGet(transition);
            persistCurrentState();
        }
    }

    private void persistCurrentState() {
        RaftNodeState currentState = state.get();
        RaftMetadata metadata = new RaftMetadata(currentState.currentTerm(), currentState.votedFor());
        metadataStore.save(metadata);
    }

    private void processProposals() {
        executor.submit(() -> {
            List<ProposalRequest> batch = new ArrayList<>();
            proposalQueue.drainTo(batch, config.maxEntriesPerAppend());

            if (batch.isEmpty()) {
                return;
            }

            RaftNodeState currentState = state.get();

            for (ProposalRequest request : batch) {
                long index = log.lastIndex() + 1;
                LogEntry entry = new LogEntry(index, currentState.currentTerm(), request.operation);

                try {
                    log.append(entry);
                    log.sync();

                    pendingProposals.put(index, request.future);

                } catch (Exception e) {
                    request.future.completeExceptionally(e);
                }
            }

            sendHeartbeats();
        });
    }

    private void sendHeartbeats() {
        RaftNodeState currentState = state.get();
        if (!currentState.isLeader()) {
            return;
        }

        logReplicator.replicateToAll(
            currentState.currentTerm(),
            currentState.commitIndex(),
            this::stepDown
        );

        updateCommitIndex();
    }

    private void updateCommitIndex() {
        RaftNodeState currentState = state.get();
        if (!currentState.isLeader()) {
            return;
        }

        long newCommitIndex = logReplicator.calculateCommitIndex(
            currentState.currentTerm(),
            currentState.commitIndex()
        );

        if (newCommitIndex > currentState.commitIndex()) {
            state.updateAndGet(s -> s.withCommitIndex(newCommitIndex));
            synchronized (commitNotifier) {
                commitNotifier.notifyAll();
            }
        }
    }

    private void startElection() {
        if (closed) {
            return;
        }

        RaftNodeState currentState = state.get();
        if (currentState.isLeader()) {
            return;
        }

        long newTerm = currentState.currentTerm() + 1;
        transitionState(s -> s.becomeCandidate(newTerm, nodeId));

        currentState = state.get();
        AtomicLong votesReceived = new AtomicLong(1);

        logger.info("Election started: votes={}, quorum={}", votesReceived.get(), config.cluster().quorum());

        if (votesReceived.get() >= config.cluster().quorum()) {
            logger.info("Single-node quorum reached, becoming leader");
            becomeLeader();
            return;
        }

        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();

        RequestVoteRequest request = new RequestVoteRequest(
            currentState.currentTerm(),
            nodeId,
            lastLogIndex,
            lastLogTerm
        );

        for (String peerId : config.peerNodeIds()) {
            transport.requestVote(peerId, request).thenAccept(response -> {
                if (response.term() > state.get().currentTerm()) {
                    stepDown(response.term());
                    return;
                }

                if (response.voteGranted() && state.get().isCandidate()) {
                    long votes = votesReceived.incrementAndGet();

                    if (votes >= config.cluster().quorum()) {
                        becomeLeader();
                    }
                }
            }).exceptionally(ex -> null);
        }

        electionTimer.reset(this::startElection);
    }

    private void becomeLeader() {
        synchronized (stateLock) {
            RaftNodeState currentState = state.get();
            if (!currentState.isCandidate()) {
                logger.warn("becomeLeader: not a candidate, role={}", currentState.role());
                return;
            }

            RaftNodeState newState = currentState.becomeLeader(currentState.currentTerm(), nodeId);
            if (!state.compareAndSet(currentState, newState)) {
                logger.warn("becomeLeader: CAS failed");
                return;
            }
            logger.info("Became leader for term {}", newState.currentTerm());
            persistCurrentState();

            logReplicator.resetPeerStates(log.lastIndex());

            electionTimer.cancel();
            heartbeatScheduler.start(this::sendHeartbeats);

            sendHeartbeats();
        }
    }

    private void stepDown(long term) {
        transitionState(s -> s.becomeFollower(term));

        heartbeatScheduler.cancel();
        electionTimer.start(this::startElection);
    }

    private CompletableFuture<Long> confirmLeadership() {
        RaftNodeState currentState = state.get();
        long readIndex = currentState.commitIndex();

        List<CompletableFuture<Boolean>> acks = new ArrayList<>();

        for (String peerId : config.peerNodeIds()) {
            PeerReplicationState peerState = logReplicator.getPeerState(peerId);
            if (peerState == null) {
                continue;
            }

            long prevLogIndex = peerState.nextIndex() - 1;
            long prevLogTerm = prevLogIndex > 0
                ? log.get(prevLogIndex).map(LogEntry::term).orElse(0L)
                : 0;

            AppendEntriesRequest request = new AppendEntriesRequest(
                currentState.currentTerm(),
                nodeId,
                prevLogIndex,
                prevLogTerm,
                List.of(),
                currentState.commitIndex()
            );

            CompletableFuture<Boolean> ack = transport.appendEntries(peerId, request)
                .thenApply(AppendEntriesResponse::success)
                .exceptionally(ex -> false);

            acks.add(ack);
        }

        return CompletableFuture.allOf(acks.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                long ackCount = acks.stream().filter(CompletableFuture::join).count();
                if (ackCount + 1 >= config.cluster().quorum()) {
                    return readIndex;
                } else {
                    throw new NotLeaderException(Optional.empty());
                }
            });
    }

    private CompletableFuture<Void> waitForApplied(long index) {
        if (state.get().lastApplied() >= index || closed) {
            return CompletableFuture.completedFuture(null);
        }

        CompletableFuture<Void> future = new CompletableFuture<>();
        applyWaiters.computeIfAbsent(index, k -> new CopyOnWriteArrayList<>()).add(future);

        if (state.get().lastApplied() >= index || closed) {
            future.complete(null);
        }

        return future;
    }

    private void notifyWaiters(long appliedIndex) {
        applyWaiters.entrySet().removeIf(entry -> {
            if (entry.getKey() <= appliedIndex) {
                entry.getValue().forEach(future -> future.complete(null));
                return true;
            }
            return false;
        });
    }

    private void startApplyThread() {
        executor.submit(() -> {
            while (!closed) {
                try {
                    RaftNodeState currentState = state.get();

                    while (currentState.lastApplied() < currentState.commitIndex() && !closed) {
                        long nextIndex = currentState.lastApplied() + 1;

                        Optional<LogEntry> entry = log.get(nextIndex);
                        if (entry.isPresent()) {
                            stateMachine.apply(nextIndex, entry.get().command());

                            state.updateAndGet(s -> s.withLastApplied(nextIndex));

                            CompletableFuture<Void> future = pendingProposals.remove(nextIndex);
                            if (future != null) {
                                future.complete(null);
                            }

                            notifyWaiters(nextIndex);
                        }

                        currentState = state.get();
                    }

                    synchronized (commitNotifier) {
                        commitNotifier.wait(100);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("Error applying log entry", e);
                }
            }
        });
    }

    private void startSnapshotMonitor() {
        snapshotScheduler.scheduleAtFixedRate(
            this::checkAndCreateSnapshot,
            snapshotCheckInterval.toMillis(),
            snapshotCheckInterval.toMillis(),
            TimeUnit.MILLISECONDS
        );
    }

    private void checkAndCreateSnapshot() {
        if (state.get().isCandidate()) {
            return;
        }

        if (!snapshotInProgress.compareAndSet(false, true)) {
            return;
        }

        try {
            if (shouldCreateSnapshot()) {
                createSnapshotAsync();
            }
        } finally {
            snapshotInProgress.set(false);
        }
    }

    private boolean shouldCreateSnapshot() {
        long logSizeBytes = log.sizeInBytes();
        boolean sizeExceeded = logSizeBytes > config.snapshotThresholdBytes();

        boolean intervalElapsed = Duration.between(lastSnapshotTime, Instant.now())
            .compareTo(config.minSnapshotInterval()) > 0;

        return sizeExceeded && intervalElapsed;
    }

    private void createSnapshotAsync() {
        executor.submit(() -> {
            try {
                RaftNodeState currentState = state.get();
                long snapshotIndex = currentState.commitIndex();
                long snapshotTerm = log.get(snapshotIndex)
                    .map(LogEntry::term)
                    .orElse(0L);

                snapshotManager.createSnapshot(snapshotIndex, snapshotTerm);
                lastSnapshotTime = Instant.now();

            } catch (Exception e) {
                logger.error("Error creating snapshot", e);
            }
        });
    }

    private record ProposalRequest(Operation operation, CompletableFuture<Void> future) {}
}
