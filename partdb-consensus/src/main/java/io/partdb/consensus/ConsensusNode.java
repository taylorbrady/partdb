package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;
import io.partdb.raft.RaftConfiguration;
import io.partdb.raft.RaftPersistentState;
import io.partdb.raft.Raft;
import io.partdb.raft.RaftConfig;
import io.partdb.raft.RaftEvent;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftReady;
import io.partdb.raft.RaftRole;
import io.partdb.raft.RaftSnapshot;
import io.partdb.raft.transport.RaftPeerTransport;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Objects;

public final class ConsensusNode implements ConsensusRuntime {
    private static final Logger log = LoggerFactory.getLogger(ConsensusNode.class);

    private sealed interface NodeEvent {
        record Proposal(long id, Bytes data) implements NodeEvent {}
        record Raft(RaftEvent event) implements NodeEvent {}
        record Tick() implements NodeEvent {}
        record Shutdown() implements NodeEvent {}
    }

    private record PendingRpc(CompletableFuture<RaftMessage.Response> future, long createdAtTick) {}

    private record RpcKey(String peerId, Class<? extends RaftMessage.Request> requestType) {}

    private final String nodeId;
    private final Raft raft;
    private final RaftConfig raftConfig;
    private final RaftPeerTransport transport;
    private final RaftStore store;
    private final ReplicatedStateMachine stateMachine;
    private volatile RaftRole lastKnownRole;
    private volatile String lastKnownLeaderId;
    private final AtomicLong lastLeaderChangeEpochMillis = new AtomicLong();

    private final BlockingQueue<NodeEvent> events = new LinkedBlockingQueue<>();
    private final Map<RpcKey, Deque<PendingRpc>> pendingRpcs = new ConcurrentHashMap<>();

    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong proposalCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<CommitResult>> pendingProposals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<CommitResult>> pendingCommits = new ConcurrentHashMap<>();

    private final AtomicLong readContextCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<ReadResult>> pendingReads = new ConcurrentHashMap<>();
    private final ApplyTracker applyTracker = new ApplyTracker();

    private final ExecutorService ioExecutor;
    private final ScheduledExecutorService tickScheduler;
    private final Thread eventLoop;
    private volatile boolean running = true;

    private ConsensusNode(
        String nodeId,
        Raft raft,
        RaftConfig raftConfig,
        RaftPeerTransport transport,
        RaftStore store,
        ReplicatedStateMachine stateMachine,
        Duration tickInterval
    ) {
        this.nodeId = nodeId;
        this.raft = raft;
        this.raftConfig = raftConfig;
        this.transport = transport;
        this.store = store;
        this.stateMachine = stateMachine;
        this.lastKnownRole = raft.role();
        this.lastKnownLeaderId = raft.leaderId().orElse(null);

        this.ioExecutor = Executors.newVirtualThreadPerTaskExecutor();

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
                events.offer(new NodeEvent.Raft(new RaftEvent.Tick()));
            },
            tickInterval.toMillis(),
            tickInterval.toMillis(),
            TimeUnit.MILLISECONDS
        );

        transport.start(this::handleIncomingRpc);
    }

    public static ConsensusNode open(
        Path dataDirectory,
        ConsensusConfig config,
        RaftPeerTransport transport,
        ReplicatedStateMachine stateMachine
    ) {
        Objects.requireNonNull(dataDirectory, "dataDirectory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        var configuration = RaftConfigurationMapper.toRaftConfiguration(config.membership());
        RaftStore store = openStore(dataDirectory, configuration);
        return openRuntime(
            config.nodeId(),
            configuration,
            config.toRaftConfig(),
            config.tickInterval(),
            transport,
            store,
            stateMachine
        );
    }

    public CompletableFuture<CommitResult> commit(Bytes data) {
        Objects.requireNonNull(data, "data must not be null");
        return proposeInternal(data);
    }

    public CompletableFuture<Long> linearizableBarrier() {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }
        return linearizableReadIndexInternal().thenCompose(result -> waitForAppliedInternal(result.index()));
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
        return RaftConfigurationMapper.toClusterMembership(raft.configuration());
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
                    case NodeEvent.Raft(RaftEvent re) -> {
                        var ready = raft.step(re);
                        processReady(ready);
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
        long threshold = currentTick - raftConfig.electionTimeoutMax();

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

        var ready = raft.step(new RaftEvent.Propose(data));

        if (!ready.persistence().entries().isEmpty()) {
            long index = ready.persistence().entries().getLast().index();
            pendingCommits.put(index, future);
        } else {
            future.completeExceptionally(new ConsensusException.NotLeader(raft.leaderId().orElse(null)));
        }

        processReady(ready);
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

        events.offer(new NodeEvent.Raft(new RaftEvent.Receive(from, request)));

        return responseFuture;
    }

    private void processReady(RaftReady ready) {
        if (!ready.hasWork()) {
            return;
        }

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

        var persistence = ready.persistence();
        if (persistence.hasWork()) {
            store.append(persistence.persistentState().orElse(null), persistence.entries());

            persistence.incomingSnapshot().ifPresent(snapshot -> {
                store.saveSnapshot(snapshot);
                stateMachine.restore(snapshot.index(), snapshot.data());
                applyTracker.advance(snapshot.index());
            });

            if (persistence.requiresSync()) {
                store.sync();
            }

            if (!persistence.entries().isEmpty()) {
                var lastPersisted = persistence.entries().getLast();
                raft.acknowledgeEntryPersistence(lastPersisted.index(), lastPersisted.term());
            }
            persistence.incomingSnapshot().ifPresent(_ -> raft.acknowledgeSnapshotPersistence());
        }

        for (var outbound : ready.messages()) {
            switch (outbound.message()) {
                case RaftMessage.Request request -> {
                    ioExecutor.execute(() -> sendRequest(outbound.to(), request));
                }
                case RaftMessage.Response response -> {
                    completeRpc(outbound.to(), response);
                }
            }
        }

        for (var entry : ready.application().entries()) {
            ApplyResult application = stateMachine.apply(entry.index(), entry.data());
            var commitFuture = pendingCommits.remove(entry.index());
            if (commitFuture != null) {
                switch (application) {
                    case ApplyResult.Applied(var result) ->
                        commitFuture.complete(new CommitResult.Applied(entry.index(), entry.term(), result));
                    case ApplyResult.Rejected(var result) ->
                        commitFuture.complete(new CommitResult.Rejected(entry.index(), entry.term(), result));
                }
            }
        }
        long appliedThroughIndex = ready.application().appliedThroughIndex();
        if (appliedThroughIndex > 0) {
            raft.acknowledgeApplication(appliedThroughIndex);
            applyTracker.advance(appliedThroughIndex);
        }

        for (var readState : ready.application().readStates()) {
            long id = bytesToLong(readState.context());
            var future = pendingReads.remove(id);
            if (future != null) {
                if (readState.index() == 0) {
                    future.completeExceptionally(new ConsensusException.NotLeader(raft.leaderId().orElse(null)));
                } else {
                    future.complete(new ReadResult(readState.index(), raft.term()));
                }
            }
        }

        ready.snapshotTransfer().ifPresent(transfer -> ioExecutor.execute(() -> sendSnapshot(transfer)));
    }

    private void sendSnapshot(RaftReady.SnapshotTransfer request) {
        var snapshot = store.snapshot();
        if (snapshot.isEmpty()) {
            Bytes data = stateMachine.snapshot();
            var newSnapshot = new RaftSnapshot(request.index(), request.term(), raft.configuration(), data);
            store.saveSnapshot(newSnapshot);
            snapshot = Optional.of(newSnapshot);
        }

        var snap = snapshot.get();
        var msg = new RaftMessage.InstallSnapshot(
            raft.term(),
            raft.leaderId().orElseThrow(),
            snap.index(),
            snap.term(),
            snap.configuration(),
            snap.data()
        );

        try {
            var response = transport.send(request.peer(), msg).join();
            events.offer(new NodeEvent.Raft(new RaftEvent.Receive(request.peer(), response)));
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
            events.offer(new NodeEvent.Raft(new RaftEvent.Receive(to, response)));
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
            case RaftMessage.ReadIndexResponse _ -> RaftMessage.ReadIndex.class;
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

    private CompletableFuture<CommitResult> proposeInternal(Bytes data) {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }
        long id = proposalCounter.incrementAndGet();
        var future = new CompletableFuture<CommitResult>();
        pendingProposals.put(id, future);
        events.offer(new NodeEvent.Proposal(id, data));
        return future;
    }

    private CompletableFuture<ReadResult> linearizableReadIndexInternal() {
        if (!running) {
            return CompletableFuture.failedFuture(new ConsensusException.Shutdown());
        }
        long id = readContextCounter.incrementAndGet();
        var future = new CompletableFuture<ReadResult>();
        pendingReads.put(id, future);
        events.offer(new NodeEvent.Raft(new RaftEvent.ReadIndex(Bytes.copyOf(longToBytes(id)))));
        return future;
    }

    private CompletableFuture<Long> waitForAppliedInternal(long index) {
        return applyTracker.waitFor(index);
    }

    private static RaftStore openStore(Path dataDirectory, RaftConfiguration configuration) {
        Path raftDir = dataDirectory.resolve("wal");
        if (Files.exists(raftDir)) {
            return DurableRaftStore.open(dataDirectory);
        }
        return DurableRaftStore.create(dataDirectory, configuration);
    }

    private static ConsensusNode openRuntime(
        String nodeId,
        RaftConfiguration configuration,
        RaftConfig raftConfig,
        Duration tickInterval,
        RaftPeerTransport transport,
        RaftStore store,
        ReplicatedStateMachine stateMachine
    ) {
        var bootstrap = store.bootstrap();
        var effectiveConfiguration = bootstrap.configuration().orElse(configuration);

        if (effectiveConfiguration == null) {
            throw new IllegalStateException("configuration is required");
        }

        var raft = Raft.builder(nodeId, effectiveConfiguration, raftConfig, store).build();

        var persistentState = bootstrap.persistentState().orElse(RaftPersistentState.INITIAL);
        var snapshot = store.snapshot();
        long snapshotIndex = snapshot.map(RaftSnapshot::index).orElse(0L);
        raft.restore(persistentState, snapshotIndex);

        snapshot.ifPresent(s -> stateMachine.restore(s.index(), s.data()));
        var recoveredApplication = raft.recoverCommittedApplication();
        for (var entry : recoveredApplication.entries()) {
            stateMachine.apply(entry.index(), entry.data());
        }

        var node = new ConsensusNode(nodeId, raft, raftConfig, transport, store, stateMachine, tickInterval);
        if (raft.lastApplied() > 0) {
            node.applyTracker.advance(raft.lastApplied());
        }
        return node;
    }
}
