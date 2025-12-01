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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

public final class RaftNode implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(RaftNode.class);

    private final String nodeId;
    private final RaftConfig config;
    private final StateMachine stateMachine;
    private final RaftLog log;
    private final RaftTransport transport;
    private final Snapshotter snapshotter;
    private final HardStateStore hardStateStore;

    private final ReentrantLock stateLock = new ReentrantLock();
    private final AtomicReference<RaftState> state;

    private final LogReplicator logReplicator;
    private final ElectionTimer electionTimer;
    private final HeartbeatScheduler heartbeatScheduler;

    private final BlockingQueue<ProposalRequest> proposalQueue;
    private final Map<Long, CompletableFuture<Void>> pendingProposals;
    private final Map<Long, List<CompletableFuture<Void>>> applyWaiters;
    private final ReentrantLock commitLock = new ReentrantLock();
    private final Condition commitCondition = commitLock.newCondition();

    private final ScheduledExecutorService scheduler;
    private final ScheduledExecutorService snapshotScheduler;
    private final ExecutorService executor;
    private final ExecutorService proposalProcessor;

    private final AtomicBoolean snapshotInProgress;
    private volatile Instant lastSnapshotTime;
    private final Duration snapshotCheckInterval;

    private volatile boolean closed;

    private RaftNode(
        String nodeId,
        RaftConfig config,
        StateMachine stateMachine,
        RaftLog log,
        RaftTransport transport,
        Snapshotter snapshotter,
        HardStateStore hardStateStore,
        AtomicReference<RaftState> state,
        LogReplicator logReplicator,
        ElectionTimer electionTimer,
        HeartbeatScheduler heartbeatScheduler,
        BlockingQueue<ProposalRequest> proposalQueue,
        Map<Long, CompletableFuture<Void>> pendingProposals,
        Map<Long, List<CompletableFuture<Void>>> applyWaiters,
        ScheduledExecutorService scheduler,
        ScheduledExecutorService snapshotScheduler,
        ExecutorService executor,
        ExecutorService proposalProcessor
    ) {
        this.nodeId = nodeId;
        this.config = config;
        this.stateMachine = stateMachine;
        this.log = log;
        this.transport = transport;
        this.snapshotter = snapshotter;
        this.hardStateStore = hardStateStore;
        this.state = state;
        this.logReplicator = logReplicator;
        this.electionTimer = electionTimer;
        this.heartbeatScheduler = heartbeatScheduler;
        this.proposalQueue = proposalQueue;
        this.pendingProposals = pendingProposals;
        this.applyWaiters = applyWaiters;
        this.scheduler = scheduler;
        this.snapshotScheduler = snapshotScheduler;
        this.executor = executor;
        this.proposalProcessor = proposalProcessor;
        this.snapshotInProgress = new AtomicBoolean(false);
        this.lastSnapshotTime = Instant.MIN;
        this.snapshotCheckInterval = Duration.ofSeconds(10);
        this.closed = false;
    }

    public static RaftNode start(
        RaftConfig config,
        StateMachine stateMachine,
        RaftTransport transport
    ) {
        String nodeId = config.nodeId();

        RaftLog log = SegmentedRaftLog.open(
            new RaftLogConfig(config.dataDirectory().resolve("raft-log"), RaftLogConfig.DEFAULT_SEGMENT_SIZE)
        );

        Snapshotter snapshotter = new Snapshotter(
            config.dataDirectory().resolve("snapshots"),
            stateMachine,
            log
        );

        HardStateStore hardStateStore;
        AtomicReference<RaftState> state;
        try {
            hardStateStore = HardStateStore.open(config.dataDirectory());
            HardState hard = hardStateStore.load();
            state = new AtomicReference<>(RaftState.fromHard(hard));
        } catch (Exception e) {
            throw new RaftException.MetadataException("Failed to initialize hard state store", e);
        }

        BlockingQueue<ProposalRequest> proposalQueue = new LinkedBlockingQueue<>(config.maxProposalQueueSize());
        Map<Long, CompletableFuture<Void>> pendingProposals = new ConcurrentHashMap<>();
        Map<Long, List<CompletableFuture<Void>>> applyWaiters = new ConcurrentHashMap<>();

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, Thread.ofVirtual().factory());
        ScheduledExecutorService snapshotScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("raft-snapshot-", 0).factory()
        );
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        ExecutorService proposalProcessor = Executors.newSingleThreadExecutor(
            Thread.ofVirtual().name("proposal-processor").factory()
        );

        LogReplicator logReplicator = new LogReplicator(
            nodeId,
            config.cluster(),
            log,
            transport,
            config,
            executor
        );

        long electionTimeoutBase = config.electionTimeoutMinMs();
        long electionTimeoutJitter = config.electionTimeoutMaxMs() - config.electionTimeoutMinMs();
        ElectionTimer electionTimer = new ElectionTimer(scheduler, electionTimeoutBase, electionTimeoutJitter);

        HeartbeatScheduler heartbeatScheduler = new HeartbeatScheduler(scheduler, config.heartbeatIntervalMs());

        RaftNode node = new RaftNode(
            nodeId,
            config,
            stateMachine,
            log,
            transport,
            snapshotter,
            hardStateStore,
            state,
            logReplicator,
            electionTimer,
            heartbeatScheduler,
            proposalQueue,
            pendingProposals,
            applyWaiters,
            scheduler,
            snapshotScheduler,
            executor,
            proposalProcessor
        );

        Optional<RaftSnapshot> snapshot = snapshotter.loadLatestSnapshot();
        if (snapshot.isPresent()) {
            stateMachine.restore(snapshot.get().stateSnapshot());
            state.updateAndGet(s -> s
                .withLastApplied(snapshot.get().lastIncludedIndex())
                .withCommitIndex(snapshot.get().lastIncludedIndex())
            );
        }

        electionTimer.start(node::startElection);
        node.startApplyThread();
        node.startSnapshotMonitor();

        return node;
    }

    public CompletableFuture<Void> propose(Operation operation) {
        RaftState currentState = state.get();
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
        RaftState currentState = state.get();
        if (!currentState.isLeader()) {
            return CompletableFuture.failedFuture(
                new NotLeaderException(Optional.ofNullable(currentState.leaderId()))
            );
        }

        return confirmLeadership()
            .thenCompose(this::waitForApplied)
            .thenApply(_ -> stateMachine.get(key));
    }

    public CompletableFuture<Iterator<Entry>> scan(ByteArray startKey, ByteArray endKey) {
        RaftState currentState = state.get();
        if (!currentState.isLeader()) {
            return CompletableFuture.failedFuture(
                new NotLeaderException(Optional.ofNullable(currentState.leaderId()))
            );
        }

        return confirmLeadership()
            .thenCompose(this::waitForApplied)
            .thenApply(_ -> stateMachine.scan(startKey, endKey));
    }

    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        RaftState currentState = state.get();

        if (request.term() > currentState.term()) {
            stepDown(request.term());
            currentState = state.get();
        }

        if (request.term() < currentState.term()) {
            return new RequestVoteResponse(currentState.term(), false);
        }

        if (currentState.votedFor() != null && !currentState.votedFor().equals(request.candidateId())) {
            return new RequestVoteResponse(currentState.term(), false);
        }

        long lastLogIndex = log.lastIndex();
        long lastLogTerm = log.lastTerm();

        boolean logUpToDate = request.lastLogTerm() > lastLogTerm ||
            (request.lastLogTerm() == lastLogTerm && request.lastLogIndex() >= lastLogIndex);

        if (logUpToDate) {
            transitionState(s -> s.withVote(request.candidateId()));
            electionTimer.reset(this::startElection);
            return new RequestVoteResponse(state.get().term(), true);
        }

        return new RequestVoteResponse(state.get().term(), false);
    }

    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        RaftState currentState = state.get();

        if (request.term() > currentState.term()) {
            stepDown(request.term());
            currentState = state.get();
        }

        if (request.term() < currentState.term()) {
            return new AppendEntriesResponse(currentState.term(), false, 0);
        }

        electionTimer.reset(this::startElection);

        if (state.get().isCandidate()) {
            transitionState(s -> s.becomeFollower(s.term()).withLeader(request.leaderId()));
        } else {
            state.updateAndGet(s -> s.withLeader(request.leaderId()));
        }

        if (request.prevLogIndex() > 0) {
            Optional<LogEntry> prevEntry = log.get(request.prevLogIndex());

            if (prevEntry.isEmpty() || prevEntry.get().term() != request.prevLogTerm()) {
                return new AppendEntriesResponse(state.get().term(), false, 0);
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

        long finalMatchIndex = matchIndex;
        if (request.leaderCommit() > state.get().commitIndex()) {
            long newCommitIndex = Math.min(request.leaderCommit(), finalMatchIndex);
            state.updateAndGet(s -> s.withCommitIndex(newCommitIndex));
            signalCommit();
        }

        return new AppendEntriesResponse(state.get().term(), true, matchIndex);
    }

    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request) {
        RaftState currentState = state.get();

        if (request.term() > currentState.term()) {
            stepDown(request.term());
            currentState = state.get();
        }

        if (request.term() < currentState.term()) {
            return new InstallSnapshotResponse(currentState.term());
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

        snapshotter.installSnapshot(snapshot);

        state.updateAndGet(s -> s
            .withCommitIndex(request.lastIncludedIndex())
            .withLastApplied(request.lastIncludedIndex())
        );

        return new InstallSnapshotResponse(state.get().term());
    }

    @Override
    public void close() {
        closed = true;

        electionTimer.cancel();
        heartbeatScheduler.cancel();

        boolean interrupted = false;
        interrupted |= !shutdownExecutor(scheduler);
        interrupted |= !shutdownExecutor(snapshotScheduler);
        interrupted |= !shutdownExecutor(executor);
        interrupted |= !shutdownExecutor(proposalProcessor);

        if (interrupted) {
            Thread.currentThread().interrupt();
        }

        log.close();
    }

    private boolean shutdownExecutor(ExecutorService exec) {
        exec.shutdown();
        try {
            if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                exec.shutdownNow();
            }
            return true;
        } catch (InterruptedException e) {
            exec.shutdownNow();
            return false;
        }
    }

    private void transitionState(UnaryOperator<RaftState> transition) {
        stateLock.lock();
        try {
            state.updateAndGet(transition);
            persistHardState();
        } finally {
            stateLock.unlock();
        }
    }

    private void persistHardState() {
        hardStateStore.save(state.get().hard());
    }

    private void startElection() {
        if (closed) {
            return;
        }

        RaftState currentState = state.get();
        if (currentState.isLeader()) {
            return;
        }

        try {
            long newTerm = currentState.term() + 1;
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
                currentState.term(),
                nodeId,
                lastLogIndex,
                lastLogTerm
            );

            for (String peerId : config.peerNodeIds()) {
                transport.requestVote(peerId, request).thenAccept(response -> {
                    if (response.term() > state.get().term()) {
                        stepDown(response.term());
                        return;
                    }

                    if (response.voteGranted() && state.get().isCandidate()) {
                        long votes = votesReceived.incrementAndGet();

                        if (votes >= config.cluster().quorum()) {
                            becomeLeader();
                        }
                    }
                }).exceptionally(_ -> null);
            }
        } finally {
            if (!closed && !state.get().isLeader()) {
                electionTimer.reset(this::startElection);
            }
        }
    }

    private void becomeLeader() {
        stateLock.lock();
        try {
            RaftState currentState = state.get();
            if (!currentState.isCandidate()) {
                logger.warn("becomeLeader: not a candidate, role={}", currentState.role());
                return;
            }

            RaftState newState = currentState.becomeLeader(currentState.term(), nodeId);
            if (!state.compareAndSet(currentState, newState)) {
                logger.warn("becomeLeader: CAS failed");
                return;
            }
            logger.info("Became leader for term {}", newState.term());
            persistHardState();

            logReplicator.resetProgress(log.lastIndex());

            electionTimer.cancel();
            heartbeatScheduler.start(this::sendHeartbeats);

            sendHeartbeats();
        } finally {
            stateLock.unlock();
        }
    }

    private void stepDown(long term) {
        transitionState(s -> s.becomeFollower(term));

        heartbeatScheduler.cancel();
        electionTimer.start(this::startElection);
    }

    private void processProposals() {
        proposalProcessor.submit(() -> {
            List<ProposalRequest> batch = new ArrayList<>();
            proposalQueue.drainTo(batch, config.maxEntriesPerAppend());

            if (batch.isEmpty()) {
                return;
            }

            RaftState currentState = state.get();

            for (ProposalRequest request : batch) {
                long index = log.lastIndex() + 1;
                LogEntry entry = new LogEntry(index, currentState.term(), request.operation);

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
        RaftState currentState = state.get();
        if (!currentState.isLeader()) {
            return;
        }

        logReplicator.replicateToAll(
            currentState.term(),
            currentState.commitIndex(),
            this::stepDown
        );

        updateCommitIndex();
    }

    private void updateCommitIndex() {
        RaftState currentState = state.get();
        if (!currentState.isLeader()) {
            return;
        }

        long newCommitIndex = logReplicator.calculateCommitIndex(
            currentState.term(),
            currentState.commitIndex()
        );

        if (newCommitIndex > currentState.commitIndex()) {
            state.updateAndGet(s -> s.withCommitIndex(newCommitIndex));
            signalCommit();
        }
    }

    private void signalCommit() {
        commitLock.lock();
        try {
            commitCondition.signalAll();
        } finally {
            commitLock.unlock();
        }
    }

    private CompletableFuture<Long> confirmLeadership() {
        RaftState currentState = state.get();
        long readIndex = currentState.commitIndex();

        List<CompletableFuture<Boolean>> acks = new ArrayList<>();

        for (String peerId : config.peerNodeIds()) {
            Progress progress = logReplicator.getProgress(peerId);
            if (progress == null) {
                continue;
            }

            long prevLogIndex = progress.nextIndex() - 1;
            long prevLogTerm = prevLogIndex > 0
                ? log.get(prevLogIndex).map(LogEntry::term).orElse(0L)
                : 0;

            AppendEntriesRequest request = new AppendEntriesRequest(
                currentState.term(),
                nodeId,
                prevLogIndex,
                prevLogTerm,
                List.of(),
                currentState.commitIndex()
            );

            CompletableFuture<Boolean> ack = transport.appendEntries(peerId, request)
                .thenApply(AppendEntriesResponse::success)
                .exceptionally(_ -> false);

            acks.add(ack);
        }

        return CompletableFuture.allOf(acks.toArray(new CompletableFuture[0]))
            .thenApply(_ -> {
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
        applyWaiters.computeIfAbsent(index, _ -> new CopyOnWriteArrayList<>()).add(future);

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
                    RaftState currentState = state.get();

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

                    commitLock.lock();
                    try {
                        commitCondition.await(100, TimeUnit.MILLISECONDS);
                    } finally {
                        commitLock.unlock();
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

        if (shouldCreateSnapshot()) {
            createSnapshotAsync();
        } else {
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
                RaftState currentState = state.get();
                long snapshotIndex = currentState.commitIndex();
                long snapshotTerm = log.get(snapshotIndex)
                    .map(LogEntry::term)
                    .orElse(0L);

                snapshotter.createSnapshot(snapshotIndex, snapshotTerm);
                lastSnapshotTime = Instant.now();

            } catch (Exception e) {
                logger.error("Error creating snapshot", e);
            } finally {
                snapshotInProgress.set(false);
            }
        });
    }

    private record ProposalRequest(Operation operation, CompletableFuture<Void> future) {}
}
