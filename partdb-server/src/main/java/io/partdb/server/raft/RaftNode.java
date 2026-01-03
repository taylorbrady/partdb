package io.partdb.server.raft;

import io.partdb.raft.HardState;
import io.partdb.raft.Membership;
import io.partdb.raft.ProposalResult;
import io.partdb.raft.Raft;
import io.partdb.raft.RaftConfig;
import io.partdb.raft.RaftEvent;
import io.partdb.raft.RaftException;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftStorage;
import io.partdb.raft.RaftTransport;
import io.partdb.raft.ReadResult;
import io.partdb.raft.Ready;
import io.partdb.raft.Role;
import io.partdb.raft.Snapshot;
import io.partdb.raft.StateMachine;
import io.partdb.storage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
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
import java.util.function.IntUnaryOperator;

public final class RaftNode implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private sealed interface NodeEvent {
        record Proposal(long id, byte[] data) implements NodeEvent {}
        record Raft(RaftEvent event) implements NodeEvent {}
        record Tick() implements NodeEvent {}
    }

    private record PendingRpc(CompletableFuture<RaftMessage.Response> future, long createdAtTick) {}

    private record RpcKey(String peerId, Class<? extends RaftMessage.Request> requestType) {}

    private final String nodeId;
    private final Raft raft;
    private final RaftConfig config;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final StateMachine stateMachine;
    private volatile Role lastKnownRole;

    private final BlockingQueue<NodeEvent> events = new LinkedBlockingQueue<>();
    private final Map<RpcKey, Deque<PendingRpc>> pendingRpcs = new ConcurrentHashMap<>();

    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong proposalCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<ProposalResult>> pendingProposals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, CompletableFuture<ProposalResult>> pendingCommits = new ConcurrentHashMap<>();

    private final AtomicLong readContextCounter = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<ReadResult>> pendingReads = new ConcurrentHashMap<>();
    private final ApplyTracker applyTracker = new ApplyTracker();

    private final ExecutorService ioExecutor;
    private final ScheduledExecutorService tickScheduler;
    private final Thread eventLoop;
    private volatile boolean running = true;

    private RaftNode(
        String nodeId,
        Raft raft,
        RaftConfig config,
        RaftTransport transport,
        RaftStorage storage,
        StateMachine stateMachine,
        Duration tickInterval
    ) {
        this.nodeId = nodeId;
        this.raft = raft;
        this.config = config;
        this.transport = transport;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.lastKnownRole = raft.role();

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

    public static Builder builder() {
        return new Builder();
    }

    public CompletableFuture<ProposalResult> propose(byte[] data) {
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Shutdown());
        }
        long id = proposalCounter.incrementAndGet();
        var future = new CompletableFuture<ProposalResult>();
        pendingProposals.put(id, future);
        events.offer(new NodeEvent.Proposal(id, data));
        return future;
    }

    public CompletableFuture<ReadResult> linearizableReadIndex() {
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Shutdown());
        }
        long id = readContextCounter.incrementAndGet();
        var future = new CompletableFuture<ReadResult>();
        pendingReads.put(id, future);
        events.offer(new NodeEvent.Raft(new RaftEvent.ReadIndex(longToBytes(id))));
        return future;
    }

    public CompletableFuture<Long> waitForApplied(long index) {
        return applyTracker.waitFor(index);
    }

    public CompletableFuture<Long> linearizableBarrier() {
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Shutdown());
        }
        return linearizableReadIndex().thenCompose(result -> waitForApplied(result.index()));
    }

    public String nodeId() {
        return nodeId;
    }

    public boolean isLeader() {
        return raft.isLeader();
    }

    public Optional<String> leaderId() {
        return raft.leaderId();
    }

    public long commitIndex() {
        return raft.commitIndex();
    }

    public boolean isRunning() {
        return running;
    }

    public long currentTerm() {
        return raft.term();
    }

    public long lastAppliedIndex() {
        return applyTracker.lastApplied();
    }

    @Override
    public void close() {
        running = false;
        failPendingOperations(new RaftException.Shutdown());
        eventLoop.interrupt();
        tickScheduler.shutdown();
        ioExecutor.shutdown();
        transport.close();
        storage.close();

        try {
            tickScheduler.awaitTermination(5, TimeUnit.SECONDS);
            ioExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runEventLoop() {
        while (running) {
            try {
                var event = events.take();
                switch (event) {
                    case NodeEvent.Proposal(long id, byte[] data) -> handleProposal(id, data);
                    case NodeEvent.Tick() -> expireStaleRpcs();
                    case NodeEvent.Raft(RaftEvent re) -> {
                        var ready = raft.step(re);
                        processReady(ready);
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (StorageException e) {
                log.atError()
                    .addKeyValue("nodeId", nodeId)
                    .setCause(e)
                    .log("Fatal storage error, shutting down");
                failPendingOperations(new RaftException.StorageFailure(e.getMessage(), e));
                running = false;
                break;
            } catch (Exception e) {
                log.atError()
                    .addKeyValue("nodeId", nodeId)
                    .setCause(e)
                    .log("Unexpected error, shutting down");
                failPendingOperations(new RaftException.StorageFailure("Unexpected error: " + e.getMessage(), e));
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
        long threshold = currentTick - config.electionTimeoutMax();

        var keysToRemove = new ArrayList<RpcKey>();
        for (var entry : pendingRpcs.entrySet()) {
            var key = entry.getKey();
            var queue = entry.getValue();

            PendingRpc rpc;
            while ((rpc = queue.peekFirst()) != null && rpc.createdAtTick() < threshold) {
                queue.pollFirst();
                rpc.future().completeExceptionally(new RaftException.RpcTimeout(key.peerId()));
            }
            if (queue.isEmpty()) {
                keysToRemove.add(key);
            }
        }
        keysToRemove.forEach(pendingRpcs::remove);
    }

    private void handleProposal(long proposalId, byte[] data) {
        var future = pendingProposals.remove(proposalId);
        if (future == null) {
            return;
        }

        if (!raft.isLeader()) {
            future.completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
            return;
        }

        var ready = raft.step(new RaftEvent.Propose(data));

        if (!ready.persist().entries().isEmpty()) {
            long index = ready.persist().entries().getLast().index();
            pendingCommits.put(index, future);
        } else {
            future.completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
        }

        processReady(ready);
    }

    private CompletableFuture<RaftMessage.Response> handleIncomingRpc(String from, RaftMessage.Request request) {
        var responseFuture = new CompletableFuture<RaftMessage.Response>();

        var key = new RpcKey(from, request.getClass());
        var pendingRpc = new PendingRpc(responseFuture, tickCount.get());
        pendingRpcs.computeIfAbsent(key, _ -> new ConcurrentLinkedDeque<>())
            .addLast(pendingRpc);

        events.offer(new NodeEvent.Raft(new RaftEvent.Receive(from, request)));

        return responseFuture;
    }

    private void processReady(Ready ready) {
        if (!ready.hasWork()) {
            return;
        }

        Role currentRole = raft.role();
        if (currentRole != lastKnownRole) {
            log.atInfo()
                .addKeyValue("nodeId", nodeId)
                .addKeyValue("term", raft.term())
                .addKeyValue("previousRole", lastKnownRole)
                .addKeyValue("newRole", currentRole)
                .log("State transition");
            lastKnownRole = currentRole;
        }

        var persist = ready.persist();
        if (persist.hasWork()) {
            storage.append(persist.hardState(), persist.entries());

            if (persist.incomingSnapshot() != null) {
                var snapshot = persist.incomingSnapshot();
                storage.saveSnapshot(snapshot);
                stateMachine.restore(snapshot.index(), snapshot.data());
            }

            if (persist.mustSync()) {
                storage.sync();
            }
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

        long lastAppliedIndex = 0;
        long lastAppliedTerm = 0;
        for (var entry : ready.apply().entries()) {
            var commitFuture = pendingCommits.remove(entry.index());
            if (commitFuture != null) {
                commitFuture.complete(new ProposalResult(entry.index(), entry.term()));
            }
            stateMachine.apply(entry.index(), entry.data());
            lastAppliedIndex = entry.index();
            lastAppliedTerm = entry.term();
        }
        if (lastAppliedIndex > 0) {
            raft.advance(lastAppliedIndex, lastAppliedTerm);
            applyTracker.advance(lastAppliedIndex);
        }

        for (var readState : ready.apply().readStates()) {
            long id = bytesToLong(readState.context());
            var future = pendingReads.remove(id);
            if (future != null) {
                if (readState.index() == 0) {
                    future.completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
                } else {
                    future.complete(new ReadResult(readState.index(), raft.term()));
                }
            }
        }

        if (ready.snapshotToSend() != null) {
            ioExecutor.execute(() -> sendSnapshot(ready.snapshotToSend()));
        }
    }

    private void sendSnapshot(Ready.SnapshotToSend request) {
        var snapshot = storage.snapshot();
        if (snapshot.isEmpty()) {
            byte[] data = stateMachine.snapshot();
            var newSnapshot = new Snapshot(request.index(), request.term(), raft.membership(), data);
            storage.saveSnapshot(newSnapshot);
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

    private static long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
    }

    public static final class Builder {
        private String nodeId;
        private Membership membership;
        private RaftConfig config;
        private RaftTransport transport;
        private RaftStorage storage;
        private StateMachine stateMachine;
        private Duration tickInterval = Duration.ofMillis(10);
        private IntUnaryOperator random;

        private Builder() {}

        public Builder nodeId(String nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder membership(Membership membership) {
            this.membership = membership;
            return this;
        }

        public Builder config(RaftConfig config) {
            this.config = config;
            return this;
        }

        public Builder transport(RaftTransport transport) {
            this.transport = transport;
            return this;
        }

        public Builder storage(RaftStorage storage) {
            this.storage = storage;
            return this;
        }

        public Builder stateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public Builder tickInterval(Duration tickInterval) {
            this.tickInterval = tickInterval;
            return this;
        }

        public Builder random(IntUnaryOperator random) {
            this.random = random;
            return this;
        }

        public RaftNode build() {
            if (nodeId == null) throw new IllegalStateException("nodeId is required");
            if (transport == null) throw new IllegalStateException("transport is required");
            if (storage == null) throw new IllegalStateException("storage is required");
            if (stateMachine == null) throw new IllegalStateException("stateMachine is required");
            if (config == null) config = RaftConfig.defaults();
            if (random == null) random = bound -> ThreadLocalRandom.current().nextInt(bound);

            var initialState = storage.initialState();
            var effectiveMembership = initialState.membership() != null
                ? initialState.membership()
                : membership;

            if (effectiveMembership == null) {
                throw new IllegalStateException("membership is required (either from storage or builder)");
            }

            var raft = new Raft(nodeId, effectiveMembership, config, storage, random);

            var hardState = initialState.hardState() != null ? initialState.hardState() : HardState.INITIAL;
            long snapIndex = storage.firstIndex() > 1 ? storage.firstIndex() - 1 : 0;
            raft.restore(hardState, snapIndex);

            var snapshot = storage.snapshot();
            snapshot.ifPresent(s -> stateMachine.restore(s.index(), s.data()));

            return new RaftNode(nodeId, raft, config, transport, storage, stateMachine, tickInterval);
        }
    }
}
