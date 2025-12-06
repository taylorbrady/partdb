package io.partdb.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;

public final class RaftNode implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    private final Raft raft;
    private final RaftTransport transport;
    private final RaftStorage storage;
    private final StateMachine stateMachine;

    private final BlockingQueue<RaftEvent> events = new LinkedBlockingQueue<>();
    private final Map<RpcKey, Deque<CompletableFuture<RaftMessage.Response>>> pendingRpcs = new ConcurrentHashMap<>();

    private final ExecutorService ioExecutor;
    private final ScheduledExecutorService tickScheduler;
    private final Thread eventLoop;
    private volatile boolean running = true;

    private RaftNode(
        Raft raft,
        RaftTransport transport,
        RaftStorage storage,
        StateMachine stateMachine,
        Duration tickInterval
    ) {
        this.raft = raft;
        this.transport = transport;
        this.storage = storage;
        this.stateMachine = stateMachine;

        this.ioExecutor = Executors.newVirtualThreadPerTaskExecutor();

        this.tickScheduler = Executors.newSingleThreadScheduledExecutor(
            Thread.ofVirtual().name("raft-ticker-", 0).factory()
        );

        this.eventLoop = Thread.ofVirtual()
            .name("raft-event-loop")
            .start(this::runEventLoop);

        tickScheduler.scheduleAtFixedRate(
            () -> events.offer(new RaftEvent.Tick()),
            tickInterval.toMillis(),
            tickInterval.toMillis(),
            TimeUnit.MILLISECONDS
        );

        transport.start(this::handleIncomingRpc);
    }

    public static Builder builder() {
        return new Builder();
    }

    public void propose(byte[] data) {
        events.offer(new RaftEvent.Propose(data));
    }

    public boolean isLeader() {
        return raft.isLeader();
    }

    public java.util.Optional<String> leaderId() {
        return raft.leaderId();
    }

    public long commitIndex() {
        return raft.commitIndex();
    }

    private void runEventLoop() {
        while (running) {
            try {
                var event = events.take();
                var ready = raft.step(event);
                processReady(ready);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error in event loop", e);
            }
        }
    }

    private CompletableFuture<RaftMessage.Response> handleIncomingRpc(String from, RaftMessage.Request request) {
        var responseFuture = new CompletableFuture<RaftMessage.Response>();

        var key = new RpcKey(from, request.getClass());
        pendingRpcs.computeIfAbsent(key, _ -> new ConcurrentLinkedDeque<>())
            .addLast(responseFuture);

        events.offer(new RaftEvent.Receive(from, request));

        return responseFuture;
    }

    private void processReady(Ready ready) {
        if (!ready.hasWork()) {
            return;
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
            stateMachine.apply(entry.index(), entry.data());
            lastAppliedIndex = entry.index();
            lastAppliedTerm = entry.term();
        }
        if (lastAppliedIndex > 0) {
            raft.advance(lastAppliedIndex, lastAppliedTerm);
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
            snapshot = java.util.Optional.of(newSnapshot);
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
            events.offer(new RaftEvent.Receive(request.peer(), response));
        } catch (Exception e) {
            log.debug("Failed to send snapshot to {}: {}", request.peer(), e.getMessage());
        }
    }

    private void sendRequest(String to, RaftMessage.Request request) {
        try {
            var response = transport.send(to, request).join();
            events.offer(new RaftEvent.Receive(to, response));
        } catch (Exception e) {
            log.debug("Failed to send to {}: {}", to, e.getMessage());
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
            var future = queue.pollFirst();
            if (future != null) {
                future.complete(response);
            }
        }
    }

    @Override
    public void close() {
        running = false;
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

    private record RpcKey(String peerId, Class<? extends RaftMessage.Request> requestType) {}

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
            if (random == null) random = bound -> java.util.concurrent.ThreadLocalRandom.current().nextInt(bound);

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

            return new RaftNode(raft, transport, storage, stateMachine, tickInterval);
        }
    }
}
