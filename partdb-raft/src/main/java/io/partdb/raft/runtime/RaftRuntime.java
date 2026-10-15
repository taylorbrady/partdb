package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Envelope;
import io.partdb.raft.HardState;
import io.partdb.raft.Input;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Membership;
import io.partdb.raft.MembershipChange;
import io.partdb.raft.NodeId;
import io.partdb.raft.Raft;
import io.partdb.raft.Ready;
import io.partdb.raft.Role;
import io.partdb.raft.SnapshotMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Optional;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongFunction;

public final class RaftRuntime<R> implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftRuntime.class);
    private static final Executor CALLBACKS = Thread::startVirtualThread;
    private static final int MAX_BATCH = 4096;

    private final NodeId nodeId;
    private final long tickNanos;
    private final Raft raft;
    private final LogStore storage;
    private final RaftTransport transport;
    private final StateMachine<R> stateMachine;
    private final Requests requests;
    private final Snapshots snapshots;
    private final LinkedBlockingQueue<LoopEvent> events = new LinkedBlockingQueue<>();

    private HardState syncedState;
    private Role lastRole;
    private Optional<NodeId> lastLeader;
    private Instant lastLeaderChange;
    private volatile boolean running = true;
    private volatile RaftStatus status;
    private volatile Membership membership;
    private Thread loop;

    private RaftRuntime(RaftConfig config, Raft raft, LogStore storage, RaftTransport transport, StateMachine<R> stateMachine) {
        this.nodeId = config.nodeId();
        this.tickNanos = config.tickInterval().toNanos();
        this.raft = raft;
        this.storage = storage;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.requests = new Requests(raft.appliedIndex());
        this.snapshots = new Snapshots(nodeId, storage, stateMachine, transport, config.snapshots(), CALLBACKS, this::enqueue);
        this.syncedState = storage.hardState();
        this.lastRole = raft.role();
        this.lastLeader = raft.leaderId();
    }

    public static <R> RaftRuntime<R> open(Path directory, RaftConfig config, RaftTransport transport, StateMachine<R> stateMachine) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        LogStore storage = null;
        try {
            storage = LogStore.open(directory);
            var cluster = clusterFile(directory, config, storage);
            var snapshot = storage.snapshot();
            long snapshotIndex = snapshot.map(file -> file.metadata().index()).orElse(0L);
            if (snapshot.isPresent() && stateMachine.appliedIndex() < snapshotIndex) {
                Snapshots.restore(stateMachine, snapshot.get());
            }
            var raft = Raft.recover(
                config.nodeId(),
                snapshot.map(file -> file.metadata().membership()).orElse(cluster.initialMembership()),
                config.options(),
                storage,
                storage.hardState(),
                Math.max(stateMachine.appliedIndex(), snapshotIndex),
                new SplittableRandom()
            );
            var runtime = new RaftRuntime<>(config, raft, storage, transport, stateMachine);
            runtime.start();
            return runtime;
        } catch (RuntimeException e) {
            if (storage != null) {
                storage.close();
            }
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    public CompletionStage<R> propose(Bytes command) {
        Objects.requireNonNull(command, "command must not be null");
        var pending = new Requests.Pending.Proposal(new CompletableFuture<>());
        return submit(pending, id -> new Input.Propose(id, command)).thenApply(result -> (R) result);
    }

    public CompletionStage<Long> readBarrier() {
        var pending = new Requests.Pending.Read(new CompletableFuture<>());
        return submit(pending, Input.ReadIndex::new);
    }

    public CompletionStage<Membership> changeMembership(MembershipChange change) {
        Objects.requireNonNull(change, "change must not be null");
        var pending = new Requests.Pending.Change(new CompletableFuture<>());
        return submit(pending, id -> new Input.ChangeMembership(id, change));
    }

    public CompletionStage<Void> transferLeadership(NodeId target) {
        Objects.requireNonNull(target, "target must not be null");
        var pending = new Requests.Pending.Transfer(new CompletableFuture<>());
        return submit(pending, id -> new Input.TransferLeadership(id, target));
    }

    public RaftStatus status() {
        return status;
    }

    public Membership membership() {
        return membership;
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        events.add(new LoopEvent.Shutdown());
        try {
            loop.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        failPending(new RaftException.Closed());
        snapshots.discardStaged();
        storage.close();
        publishStatus();
    }

    @SuppressWarnings("unchecked")
    private <T> CompletionStage<T> submit(Requests.Pending pending, LongFunction<Input> input) {
        var future = (CompletableFuture<T>) pending.future();
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Closed());
        }
        events.add(new LoopEvent.Submit(pending, input));
        if (!running) {
            future.completeExceptionally(new RaftException.Closed());
        }
        return future.thenApplyAsync(Function.identity(), CALLBACKS);
    }

    private void enqueue(LoopEvent event) {
        if (running) {
            events.add(event);
            return;
        }
        switch (event) {
            case LoopEvent.SnapshotReceived received -> Snapshots.deleteQuietly(received.payload());
            case LoopEvent.SnapshotCreated created -> Snapshots.deleteQuietly(created.payload());
            default -> {}
        }
    }

    private void start() {
        try {
            transport.start(new Receiver());
            processReady();
            publishStatus();
        } catch (RuntimeException e) {
            running = false;
            throw e;
        }
        loop = Thread.ofVirtual().name("raft-" + nodeId).start(this::run);
    }

    private void run() {
        var batch = new ArrayList<LoopEvent>();
        long nextTick = System.nanoTime() + tickNanos;
        try {
            while (running) {
                long wait = nextTick - System.nanoTime();
                var event = wait > 0 ? events.poll(wait, TimeUnit.NANOSECONDS) : events.poll();
                if (event != null) {
                    batch.add(event);
                    events.drainTo(batch, MAX_BATCH);
                }
                for (LoopEvent next : batch) {
                    handle(next);
                }
                batch.clear();
                long now = System.nanoTime();
                if (now - nextTick >= 0) {
                    raft.step(new Input.Tick());
                    nextTick = now + tickNanos;
                }
                processReady();
                snapshots.discardStaged();
                snapshots.maybeCreate(requests.applied());
                observeLeadership();
                publishStatus();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException | Error e) {
            log.atError().addKeyValue("nodeId", nodeId).setCause(e).log("Raft runtime failed");
            running = false;
            failPending(new RaftException.Failed(e));
            publishStatus();
        }
    }

    private void handle(LoopEvent event) {
        switch (event) {
            case LoopEvent.Step(var input) -> raft.step(input);
            case LoopEvent.Submit(var pending, var input) -> raft.step(input.apply(requests.register(pending)));
            case LoopEvent.SnapshotReceived received -> {
                snapshots.received(received);
                raft.step(new Input.InstallSnapshot(received.from(), received.term(), received.info().metadata()));
            }
            case LoopEvent.SnapshotDelivered(var peer, boolean delivered) -> {
                snapshots.delivered(peer);
                raft.step(delivered ? new Input.SnapshotSent(peer) : new Input.SnapshotFailed(peer));
            }
            case LoopEvent.SnapshotCreated created -> snapshots.created(created, raft);
            case LoopEvent.Shutdown() -> {
            }
        }
    }

    private void processReady() {
        for (var ready = raft.ready(); !ready.isEmpty(); ready = raft.ready()) {
            ready.snapshot().ifPresent(this::installSnapshot);
            persist(ready);
            ready.messages().forEach(transport::send);
            ready.outcomes().forEach(requests::resolve);
            for (LogEntry entry : ready.committed()) {
                apply(entry);
            }
            ready.snapshotRequests().forEach(peer -> snapshots.send(peer, raft.term()));
            ready.persisted().ifPresent(raft::step);
        }
    }

    private void persist(Ready ready) {
        if (ready.entries().isEmpty() && ready.hardState().isEmpty()) {
            return;
        }
        storage.append(ready.hardState(), ready.entries());
        var state = ready.hardState().orElse(syncedState);
        boolean voteChanged = state.term() != syncedState.term() || !state.votedFor().equals(syncedState.votedFor());
        if (!ready.entries().isEmpty() || voteChanged) {
            storage.sync();
        }
        syncedState = state;
    }

    private void installSnapshot(SnapshotMetadata metadata) {
        snapshots.install(metadata);
        requests.superseded(metadata.index());
    }

    private void apply(LogEntry entry) {
        switch (entry) {
            case LogEntry.Command command -> requests.applied(entry, Optional.ofNullable(stateMachine.apply(command.index(), command.command())));
            case LogEntry.Configuration _, LogEntry.NoOp _ -> {
                stateMachine.skip(entry.index());
                requests.applied(entry, Optional.empty());
            }
        }
    }

    private void observeLeadership() {
        Role role = raft.role();
        var leader = raft.leaderId();
        if (role == lastRole && leader.equals(lastLeader)) {
            return;
        }
        log.atInfo()
            .addKeyValue("nodeId", nodeId)
            .addKeyValue("term", raft.term())
            .addKeyValue("previousRole", lastRole)
            .addKeyValue("role", role)
            .addKeyValue("leader", leader.orElse(null))
            .log("Raft role changed");

        var event = new LeaderChangeEvent();
        event.nodeId = nodeId.value();
        event.term = raft.term();
        event.previousRole = lastRole.name();
        event.newRole = role.name();
        event.previousLeaderId = lastLeader.map(NodeId::value).orElse(null);
        event.newLeaderId = leader.map(NodeId::value).orElse(null);
        event.commit();

        lastRole = role;
        lastLeader = leader;
        lastLeaderChange = Instant.now();
    }

    private void publishStatus() {
        membership = raft.membership();
        status = new RaftStatus(
            nodeId,
            raft.role(),
            raft.term(),
            raft.leaderId(),
            raft.commitIndex(),
            requests.applied(),
            snapshots.snapshotIndex(),
            Optional.ofNullable(lastLeaderChange),
            running
        );
    }

    private void failPending(RuntimeException error) {
        requests.failAll(error);
        for (LoopEvent event; (event = events.poll()) != null; ) {
            switch (event) {
                case LoopEvent.Submit(var pending, _) -> pending.future().completeExceptionally(error);
                case LoopEvent.SnapshotReceived received -> Snapshots.deleteQuietly(received.payload());
                case LoopEvent.SnapshotCreated created -> Snapshots.deleteQuietly(created.payload());
                case LoopEvent.Step _, LoopEvent.SnapshotDelivered _, LoopEvent.Shutdown _ -> {
                }
            }
        }
    }

    private static ClusterFile clusterFile(Path directory, RaftConfig config, LogStore storage) {
        var existing = ClusterFile.read(directory);
        if (existing.isEmpty()) {
            if (!storage.isEmpty()) {
                throw new RaftStorageException.Corruption("raft directory " + directory + " has a log but no cluster file");
            }
            var created = new ClusterFile(config.clusterId(), config.initialMembership());
            created.write(directory);
            return created;
        }
        var cluster = existing.get();
        if (!cluster.clusterId().equals(config.clusterId())) {
            throw new IllegalStateException(
                "raft directory " + directory + " belongs to cluster " + cluster.clusterId() + ", not " + config.clusterId());
        }
        return cluster;
    }

    private final class Receiver implements RaftTransport.Receiver {
        @Override
        public void receive(Envelope envelope) {
            if (running) {
                events.add(new LoopEvent.Step(new Input.Receive(envelope.from(), envelope.message())));
            }
        }

        @Override
        public RaftTransport.SnapshotSink receiveSnapshot(NodeId from, long term, SnapshotInfo snapshot) throws IOException {
            if (!running) {
                throw new RaftException.Closed();
            }
            return snapshots.stage(from, term, snapshot);
        }

        @Override
        public void unreachable(NodeId peer) {
            if (running) {
                events.add(new LoopEvent.Step(new Input.Unreachable(peer)));
            }
        }
    }
}
