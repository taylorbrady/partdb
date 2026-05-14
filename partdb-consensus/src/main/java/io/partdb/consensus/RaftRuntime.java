package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftNode;
import io.partdb.raft.RaftOptions;
import io.partdb.raft.RaftInput;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftEffects;
import io.partdb.raft.RaftRole;
import io.partdb.raft.RaftSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

public final class RaftRuntime implements ConsensusRuntime {
    private static final Logger log = LoggerFactory.getLogger(RaftRuntime.class);

    private sealed interface NodeEvent {
        record Proposal(long id, Bytes data) implements NodeEvent {}
        record RaftStep(RaftInput input) implements NodeEvent {}
        record Tick() implements NodeEvent {}
        record Shutdown() implements NodeEvent {}
    }

    private record PendingRpc(CompletableFuture<RaftMessage.Response> future, long createdAtTick) {}

    private record RpcKey(String peerId, Class<? extends RaftMessage.Request> requestType) {}

    private final String nodeId;
    private final RaftNode raft;
    private final RaftOptions raftOptions;
    private final RaftTransport transport;
    private final RaftStorage store;
    private final ReplicatedStateMachine stateMachine;
    private volatile RaftRole lastKnownRole;
    private volatile String lastKnownLeaderId;
    private final AtomicLong lastLeaderChangeEpochMillis = new AtomicLong();

    private final BlockingQueue<NodeEvent> events = new LinkedBlockingQueue<>();
    private final Map<RpcKey, Deque<PendingRpc>> pendingRpcs = new ConcurrentHashMap<>();

    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong proposalCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<ProposalResult>> pendingProposals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<ProposalResult>> pendingCommits = new ConcurrentHashMap<>();

    private final AtomicLong readContextCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<ReadBarrier>> pendingReads = new ConcurrentHashMap<>();
    private final ApplyTracker applyTracker = new ApplyTracker();
    private final RaftEffectRunner effectRunner;

    private final ExecutorService ioExecutor;
    private final ScheduledExecutorService tickScheduler;
    private final Thread eventLoop;
    private volatile boolean running = true;

    private RaftRuntime(
        String nodeId,
        RaftNode raft,
        RaftOptions raftOptions,
        RaftTransport transport,
        RaftStorage store,
        ReplicatedStateMachine stateMachine,
        Duration tickInterval
    ) {
        this.nodeId = nodeId;
        this.raft = raft;
        this.raftOptions = raftOptions;
        this.transport = transport;
        this.store = store;
        this.stateMachine = stateMachine;
        this.lastKnownRole = raft.role();
        this.lastKnownLeaderId = raft.leaderId().orElse(null);

        this.ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.effectRunner = new RaftEffectRunner(
            raft,
            store,
            stateMachine,
            pendingCommits,
            pendingReads,
            applyTracker,
            ioExecutor,
            this::recordLeadershipChangeIfNeeded,
            outbound -> sendRequest(outbound.to(), (RaftMessage.Request) outbound.message()),
            outbound -> completeRpc(outbound.to(), (RaftMessage.Response) outbound.message()),
            this::sendSnapshot
        );

        this.tickScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("raft-ticker-", 0).factory()
        );

        this.eventLoop = Thread.ofVirtual()
            .name("raft-event-loop")
            .start(this::runEventLoop);

        tickScheduler.scheduleAtFixedRate(
            () -> {
                tickCount.incrementAndGet();
                events.offer(new NodeEvent.Tick());
                events.offer(new NodeEvent.RaftStep(new RaftInput.Tick()));
            },
            tickInterval.toMillis(),
            tickInterval.toMillis(),
            TimeUnit.MILLISECONDS
        );

        transport.start(this::handleIncomingRpc);
    }

    public static RaftRuntime open(
        Path dataDirectory,
        ConsensusConfig config,
        RaftTransport transport,
        ReplicatedStateMachine stateMachine
    ) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        var membership = RaftMembershipMapper.toRaftMembership(config.membership());
        RaftStorage store = openStore(dataDirectory, membership);
        return openRuntime(
            config.nodeId(),
            membership,
            config.toRaftOptions(),
            config.tickInterval(),
            transport,
            store,
            stateMachine
        );
    }

    public CompletableFuture<ProposalResult> propose(Bytes data) {
        Objects.requireNonNull(data, "data must not be null");
        return proposeInternal(data);
    }

    public CompletableFuture<ReadBarrier> readBarrier() {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }
        return linearizableReadIndexInternal()
            .thenCompose(barrier -> waitForAppliedInternal(barrier.index()).thenApply(_ -> barrier));
    }

    public ConsensusStatus status() {
        return new ConsensusStatus(
            nodeId,
            ConsensusRole.fromRaftRole(lastKnownRole),
            raft.term(),
            raft.leaderId(),
            raft.commitIndex(),
            applyTracker.lastApplied(),
            lastLeaderChangeEpochMillis.get(),
            running
        );
    }

    public ClusterMembership membership() {
        return RaftMembershipMapper.toClusterMembership(raft.membership());
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }

        running = false;
        failPendingOperations(new ConsensusException.Shutdown());
        tickScheduler.shutdown();
        transport.close();
        events.offer(new NodeEvent.Shutdown());

        try {
            eventLoop.join(TimeUnit.SECONDS.toMillis(5));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        ioExecutor.shutdown();

        try {
            tickScheduler.awaitTermination(5, TimeUnit.SECONDS);
            ioExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        store.close();
    }

    private void runEventLoop() {
        while (running) {
            try {
                var event = events.take();
                switch (event) {
                    case NodeEvent.Proposal(long id, Bytes data) -> handleProposal(id, data);
                    case NodeEvent.Tick() -> expireStaleRpcs();
                    case NodeEvent.Shutdown() -> {
                    }
                    case NodeEvent.RaftStep(RaftInput input) -> {
                        var effects = raft.step(input);
                        effectRunner.process(effects);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (ConsensusStorageException e) {
                log.atError()
                    .addKeyValue("nodeId", nodeId)
                    .setCause(e)
                    .log("Fatal storage error, shutting down");
                failPendingOperations(new ConsensusException.StorageFailure(e.getMessage(), e));
                running = false;
                break;
            } catch (Exception e) {
                log.atError()
                    .addKeyValue("nodeId", nodeId)
                    .setCause(e)
                    .log("Unexpected error, shutting down");
                failPendingOperations(new ConsensusException.StorageFailure("Unexpected error: " + e.getMessage(), e));
                running = false;
                break;
            }
        }
    }

    private void failPendingOperations(Throwable cause) {
        pendingProposals.forEach((id, future) -> future.completeExceptionally(cause));
        pendingProposals.clear();

        pendingCommits.forEach((index, future) -> future.completeExceptionally(cause));
        pendingCommits.clear();

        pendingReads.forEach((id, future) -> future.completeExceptionally(cause));
        pendingReads.clear();

        pendingRpcs.forEach((key, queue) -> {
            PendingRpc rpc;
            while ((rpc = queue.pollFirst()) != null) {
                rpc.future().completeExceptionally(cause);
            }
        });
        pendingRpcs.clear();

        applyTracker.failAll(cause);
    }

    private void expireStaleRpcs() {
        long currentTick = tickCount.get();
        long threshold = currentTick - raftOptions.electionTimeoutMax();

        var keysToRemove = new ArrayList<RpcKey>();
        for (var entry : pendingRpcs.entrySet()) {
            var key = entry.getKey();
            var queue = entry.getValue();

            PendingRpc rpc;
            while ((rpc = queue.peekFirst()) != null && rpc.createdAtTick() < threshold) {
                queue.pollFirst();
                rpc.future().completeExceptionally(new ConsensusException.RpcTimeout(key.peerId()));
            }
            if (queue.isEmpty()) {
                keysToRemove.add(key);
            }
        }
        keysToRemove.forEach(pendingRpcs::remove);
    }

    private void handleProposal(long proposalId, Bytes data) {
        var future = pendingProposals.remove(proposalId);
        if (future == null) {
            return;
        }

        if (!raft.isLeader()) {
            future.completeExceptionally(new ConsensusException.NotLeader(raft.leaderId().orElse(null)));
            return;
        }

        var effects = raft.step(new RaftInput.EntryProposed(data));

        if (!effects.persistence().entries().isEmpty()) {
            long index = effects.persistence().entries().getLast().index();
            pendingCommits.put(index, future);
        } else {
            future.completeExceptionally(new ConsensusException.NotLeader(raft.leaderId().orElse(null)));
        }

        effectRunner.process(effects);
    }

    private CompletableFuture<RaftMessage.Response> handleIncomingRpc(String from, RaftMessage.Request request) {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }

        var responseFuture = new CompletableFuture<RaftMessage.Response>();

        var key = new RpcKey(from, request.getClass());
        var pendingRpc = new PendingRpc(responseFuture, tickCount.get());
        pendingRpcs.computeIfAbsent(key, _ -> new ConcurrentLinkedDeque<>())
            .addLast(pendingRpc);

        events.offer(new NodeEvent.RaftStep(new RaftInput.MessageReceived(from, request)));

        return responseFuture;
    }

    private void recordLeadershipChangeIfNeeded() {
        RaftRole currentRole = raft.role();
        String currentLeaderId = raft.leaderId().orElse(null);
        if (currentRole != lastKnownRole || !Objects.equals(currentLeaderId, lastKnownLeaderId)) {
            log.atInfo()
                .addKeyValue("nodeId", nodeId)
                .addKeyValue("term", raft.term())
                .addKeyValue("previousRole", lastKnownRole)
                .addKeyValue("newRole", currentRole)
                .log("State transition");

            NodeLeaderChangeEvent event = new NodeLeaderChangeEvent();
            event.nodeId = nodeId;
            event.term = raft.term();
            event.previousRole = lastKnownRole.name();
            event.newRole = currentRole.name();
            event.previousLeaderId = lastKnownLeaderId;
            event.newLeaderId = currentLeaderId;
            event.commit();

            lastKnownRole = currentRole;
            lastKnownLeaderId = currentLeaderId;
            lastLeaderChangeEpochMillis.set(System.currentTimeMillis());
        }
    }

    private void sendSnapshot(RaftEffects.SnapshotNeeded request) {
        var snapshot = store.snapshot();
        if (snapshot.isEmpty()) {
            Bytes data = stateMachine.snapshot().data();
            var newSnapshot = new RaftSnapshot(request.index(), request.term(), raft.membership(), data);
            store.saveSnapshot(newSnapshot);
            snapshot = Optional.of(newSnapshot);
        }

        var snap = snapshot.get();
        var msg = new RaftMessage.InstallSnapshot(
            raft.term(),
            raft.leaderId().orElseThrow(),
            snap.index(),
            snap.term(),
            snap.membership(),
            snap.data()
        );

        try {
            var response = transport.send(request.peer(), msg).join();
            events.offer(new NodeEvent.RaftStep(new RaftInput.MessageReceived(request.peer(), response)));
        } catch (Exception e) {
            log.atDebug()
                .addKeyValue("nodeId", nodeId)
                .addKeyValue("peer", request.peer())
                .addKeyValue("error", e.getMessage())
                .log("Failed to send snapshot");
        }
    }

    private void sendRequest(String to, RaftMessage.Request request) {
        try {
            var response = transport.send(to, request).join();
            events.offer(new NodeEvent.RaftStep(new RaftInput.MessageReceived(to, response)));
        } catch (Exception e) {
            log.atDebug()
                .addKeyValue("nodeId", nodeId)
                .addKeyValue("peer", to)
                .addKeyValue("error", e.getMessage())
                .log("Failed to send request");
        }
    }

    private void completeRpc(String to, RaftMessage.Response response) {
        Class<? extends RaftMessage.Request> requestType = switch (response) {
            case RaftMessage.AppendEntriesResponse _ -> RaftMessage.AppendEntries.class;
            case RaftMessage.RequestVoteResponse _ -> RaftMessage.RequestVote.class;
            case RaftMessage.InstallSnapshotResponse _ -> RaftMessage.InstallSnapshot.class;
            case RaftMessage.PreVoteResponse _ -> RaftMessage.PreVote.class;
            case RaftMessage.ReadIndexResponse _ -> RaftMessage.ReadIndexRequested.class;
        };

        var key = new RpcKey(to, requestType);
        var queue = pendingRpcs.get(key);
        if (queue != null) {
            var pendingRpc = queue.pollFirst();
            if (pendingRpc != null) {
                pendingRpc.future().complete(response);
            }
            if (queue.isEmpty()) {
                pendingRpcs.remove(key);
            }
        }
    }

    private static byte[] longToBytes(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static long bytesToLong(Bytes bytes) {
        return ByteBuffer.wrap(bytes.toByteArray()).getLong();
    }

    private CompletableFuture<ProposalResult> proposeInternal(Bytes data) {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }
        long id = proposalCounter.incrementAndGet();
        var future = new CompletableFuture<ProposalResult>();
        pendingProposals.put(id, future);
        events.offer(new NodeEvent.Proposal(id, data));
        return future;
    }

    private CompletableFuture<ReadBarrier> linearizableReadIndexInternal() {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }
        long id = readContextCounter.incrementAndGet();
        var future = new CompletableFuture<ReadBarrier>();
        pendingReads.put(id, future);
        events.offer(new NodeEvent.RaftStep(new RaftInput.ReadIndexRequested(Bytes.copyOf(longToBytes(id)))));
        return future;
    }

    private CompletableFuture<Long> waitForAppliedInternal(long index) {
        return applyTracker.waitFor(index);
    }

    private static RaftStorage openStore(Path dataDirectory, RaftMembership membership) {
        Path raftDir = dataDirectory.resolve("wal");
        if (Files.exists(raftDir)) {
            return FileRaftStorage.open(dataDirectory);
        }
        return FileRaftStorage.create(dataDirectory, membership);
    }

    private static RaftRuntime openRuntime(
        String nodeId,
        RaftMembership membership,
        RaftOptions raftOptions,
        Duration tickInterval,
        RaftTransport transport,
        RaftStorage store,
        ReplicatedStateMachine stateMachine
    ) {
        var bootstrap = store.bootstrap();
        var effectiveMembership = bootstrap.membership().orElse(membership);

        if (!effectiveMembership.isMember(nodeId)) {
            throw new IllegalStateException("membership must include nodeId");
        }

        var hardState = bootstrap.hardState().orElse(RaftHardState.INITIAL);
        var snapshot = store.snapshot();
        long snapshotIndex = snapshot.map(RaftSnapshot::index).orElse(0L);
        var raft = RaftNode.builder(nodeId, effectiveMembership, raftOptions, store)
            .electionJitter(bound -> ThreadLocalRandom.current().nextInt(bound))
            .restoredFrom(hardState, snapshotIndex)
            .build();

        snapshot.ifPresent(s -> stateMachine.restore(StoredSnapshot.fromRaftSnapshot(s)));
        var recoveredEffects = raft.step(new RaftInput.ReplayCommitted());
        for (var entry : recoveredEffects.application().entries()) {
            stateMachine.apply(new CommittedCommand(entry.index(), entry.term(), entry.data()));
        }
        if (recoveredEffects.application().appliedThroughIndex() > 0) {
            raft.step(new RaftInput.Applied(recoveredEffects.application().appliedThroughIndex()));
        }

        var node = new RaftRuntime(nodeId, raft, raftOptions, transport, store, stateMachine, tickInterval);
        if (raft.lastApplied() > 0) {
            node.applyTracker.advance(raft.lastApplied());
        }
        return node;
    }
}
