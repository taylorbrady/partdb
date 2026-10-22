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

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.LongFunction;

import static java.util.Map.entry;

public final class RaftRuntime<R> implements AutoCloseable {

    private static final Logger log = System.getLogger(RaftRuntime.class.getName());
    private static final Executor CALLBACKS = Thread::startVirtualThread;
    private static final int MAX_BATCH = 4096;
    private static final Duration SLOW_SYNC = Duration.ofSeconds(1);
    private static final Duration SLOW_APPLY = Duration.ofMillis(100);

    private final NodeId nodeId;
    private final ClusterId clusterId;
    private final FileLock lock;
    private final long tickNanos;
    private final Raft raft;
    private final LogStore storage;
    private final RaftTransport transport;
    private final StateMachine<R> stateMachine;
    private final Requests<R> requests;
    private final Snapshots snapshots;
    private final LogRetention retention;
    private final LinkedBlockingQueue<LoopEvent> events = new LinkedBlockingQueue<>();

    private HardState syncedState;
    private Role lastRole;
    private Optional<NodeId> lastLeader;
    private Instant lastLeaderChange;
    private final AtomicBoolean flushing = new AtomicBoolean();
    private long observedDurable = -1;
    private volatile boolean running = true;
    private volatile RaftStatus status;
    private volatile Membership membership;
    private Thread loop;

    private RaftRuntime(
        RaftConfig config,
        Identity identity,
        FileLock lock,
        Raft raft,
        LogStore storage,
        SnapshotFiles files,
        RaftTransport transport,
        StateMachine<R> stateMachine
    ) {
        this.nodeId = config.nodeId();
        this.clusterId = identity.clusterId();
        this.lock = lock;
        this.tickNanos = config.tickInterval().toNanos();
        this.raft = raft;
        this.storage = storage;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.requests = new Requests<>(raft.appliedIndex());
        Executor snapshotThreads = task -> Thread.ofPlatform().name("raft-snapshot-" + nodeId).daemon(true).start(task);
        this.snapshots = new Snapshots(storage, files, stateMachine, transport, snapshotThreads, this::enqueue);
        this.retention = config.retention();
        this.syncedState = storage.hardState();
        this.lastRole = raft.role();
        this.lastLeader = raft.leaderId();
    }

    public static <R> RaftRuntime<R> open(Path directory, RaftConfig config, RaftTransport transport, StateMachine<R> stateMachine) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        Disk.createPrivateDirectory(directory);
        var lock = Disk.lock(directory);
        LogStore storage = null;
        try {
            storage = LogStore.open(directory);
            var files = SnapshotFiles.open(directory.resolve("snap"));
            var identity = Identity.open(directory, config, storage);
            Snapshots.recover(stateMachine, storage, files);
            long applied = stateMachine.appliedIndex();
            if (applied < storage.firstIndex() - 1 || applied > storage.lastIndex()) {
                throw new RaftStorageException.Corruption("state machine applied index " + applied + " is outside the raft log ["
                    + (storage.firstIndex() - 1) + ", " + storage.lastIndex() + "] in " + directory);
            }
            var raft = Raft.recover(
                config.nodeId(),
                storage.base().map(SnapshotMetadata::membership).orElse(identity.initialMembership()),
                config.options(),
                storage,
                storage.hardState(),
                applied,
                new SplittableRandom()
            );
            log.log(Level.INFO, "Recovered raft log",
                entry("clusterId", identity.clusterId()),
                entry("term", storage.hardState().term()),
                entry("votedFor", storage.hardState().votedFor()),
                entry("commitIndex", storage.hardState().commit()),
                entry("appliedIndex", applied),
                entry("firstIndex", storage.firstIndex()),
                entry("lastIndex", storage.lastIndex()));
            var runtime = new RaftRuntime<>(config, identity, lock, raft, storage, files, transport, stateMachine);
            runtime.start();
            return runtime;
        } catch (RuntimeException e) {
            if (storage != null) {
                storage.close();
            }
            Disk.unlock(lock);
            throw e;
        }
    }

    public CompletionStage<R> propose(Bytes command) {
        Objects.requireNonNull(command, "command must not be null");
        var future = new CompletableFuture<R>();
        return submit(future, new Requests.Pending.Proposal<>(future), id -> new Input.Propose(id, command));
    }

    public CompletionStage<Long> readBarrier() {
        var future = new CompletableFuture<Long>();
        return submit(future, new Requests.Pending.Read<>(future), Input.ReadIndex::new);
    }

    public CompletionStage<Membership> changeMembership(MembershipChange change) {
        Objects.requireNonNull(change, "change must not be null");
        var future = new CompletableFuture<Membership>();
        return submit(future, new Requests.Pending.Change<>(future), id -> new Input.ChangeMembership(id, change));
    }

    public CompletionStage<Void> transferLeadership(NodeId target) {
        Objects.requireNonNull(target, "target must not be null");
        var future = new CompletableFuture<Void>();
        return submit(future, new Requests.Pending.Transfer<>(future), id -> new Input.TransferLeadership(id, target));
    }

    public CompletionStage<Void> stepDown() {
        var future = new CompletableFuture<Void>();
        return submit(future, new Requests.Pending.Transfer<>(future), Input.StepDown::new);
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
        snapshots.close();
        storage.close();
        Disk.unlock(lock);
        publishStatus();
    }

    private <T> CompletionStage<T> submit(CompletableFuture<T> future, Requests.Pending<R> pending, LongFunction<Input> input) {
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Closed());
        }
        events.add(new LoopEvent.Submit(future, () -> raft.step(input.apply(requests.register(pending)))));
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
            case LoopEvent.SnapshotReceived received -> SnapshotFiles.deleteQuietly(received.payload());
            case LoopEvent.SnapshotGenerated generated -> SnapshotFiles.deleteQuietly(generated.payload());
            default -> {}
        }
    }

    private void start() {
        try {
            transport.start(clusterId, new Receiver());
            processReady();
            publishStatus();
        } catch (RuntimeException e) {
            running = false;
            throw e;
        }
        loop = Thread.ofPlatform().name("raft-" + nodeId).daemon(true).start(this::run);
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
                maintainLog();
                observeLeadership();
                publishStatus();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException | Error e) {
            log.log(Level.ERROR, "Raft runtime failed", e);
            running = false;
            failPending(new RaftException.Failed(e));
            publishStatus();
        }
    }

    private void handle(LoopEvent event) {
        switch (event) {
            case LoopEvent.Step(var input) -> raft.step(input);
            case LoopEvent.Submit(_, var register) -> register.run();
            case LoopEvent.SnapshotReceived received -> {
                snapshots.received(received);
                raft.step(new Input.InstallSnapshot(received.from(), received.term(), received.info().metadata()));
            }
            case LoopEvent.SnapshotDelivered(var peer, boolean delivered) -> {
                snapshots.delivered(peer);
                raft.step(delivered ? new Input.SnapshotSent(peer) : new Input.SnapshotFailed(peer));
            }
            case LoopEvent.SnapshotGenerated generated -> snapshots.generated(generated, raft);
            case LoopEvent.Shutdown() -> {
            }
        }
    }

    private void processReady() {
        for (var ready = raft.ready(); !ready.isEmpty(); ready = raft.ready()) {
            ready.messages().forEach(transport::send);
            ready.snapshot().ifPresent(this::installSnapshot);
            persist(ready);
            ready.persistedMessages().forEach(transport::send);
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
            long started = System.nanoTime();
            storage.sync();
            var elapsed = Duration.ofNanos(System.nanoTime() - started);
            if (elapsed.compareTo(SLOW_SYNC) > 0) {
                log.log(Level.WARNING, "Slow raft log sync", entry("entries", ready.entries().size()), entry("duration", elapsed));
            }
        }
        syncedState = state;
    }

    private void installSnapshot(SnapshotMetadata metadata) {
        snapshots.install(metadata);
        requests.superseded(metadata.index());
    }

    private void maintainLog() {
        long durable = Math.min(stateMachine.durableIndex(), requests.applied());
        if (durable != observedDurable) {
            observedDurable = durable;
            long base = storage.compactionIndex(durable, retention);
            if (base >= storage.firstIndex()) {
                storage.compact(new SnapshotMetadata(base, storage.term(base), raft.membershipAt(base)));
                log.log(Level.DEBUG, "Compacted raft log", entry("index", base));
            }
        }
        boolean oversized = storage.retainedEntries() > 2 * retention.entries() || storage.retainedBytes() > 2 * retention.bytes();
        if (oversized && requests.applied() > durable && flushing.compareAndSet(false, true)) {
            stateMachine.flush().whenComplete((_, error) -> {
                flushing.set(false);
                if (error != null) {
                    log.log(Level.WARNING, "State machine flush failed", error);
                }
            });
        }
    }

    private void apply(LogEntry entry) {
        long started = System.nanoTime();
        switch (entry) {
            case LogEntry.Command command -> requests.applied(entry, Optional.ofNullable(stateMachine.apply(command.index(), command.command())));
            case LogEntry.Configuration _, LogEntry.NoOp _ -> {
                stateMachine.skip(entry.index());
                requests.applied(entry, Optional.empty());
            }
        }
        var elapsed = Duration.ofNanos(System.nanoTime() - started);
        if (elapsed.compareTo(SLOW_APPLY) > 0) {
            log.log(Level.WARNING, "Slow state machine apply", entry("index", entry.index()), entry("duration", elapsed));
        }
    }

    private void observeLeadership() {
        Role role = raft.role();
        var leader = raft.leaderId();
        if (role == lastRole && leader.equals(lastLeader)) {
            return;
        }
        log.log(Level.INFO, role != lastRole ? "Raft role changed" : leader.isPresent() ? "Raft leader changed" : "Raft leader lost",
            entry("term", raft.term()),
            entry("role", role),
            entry("previousRole", Optional.of(lastRole).filter(previous -> previous != role)),
            entry("leader", leader));

        var change = new LeaderChangeEvent();
        change.nodeId = nodeId.value();
        change.term = raft.term();
        change.previousRole = lastRole.name();
        change.newRole = role.name();
        change.previousLeaderId = lastLeader.map(NodeId::value).orElse(null);
        change.newLeaderId = leader.map(NodeId::value).orElse(null);
        change.commit();

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
            storage.firstIndex() - 1,
            Optional.ofNullable(lastLeaderChange),
            running
        );
    }

    private void failPending(RuntimeException error) {
        requests.failAll(error);
        for (LoopEvent event; (event = events.poll()) != null; ) {
            switch (event) {
                case LoopEvent.Submit(var future, _) -> future.completeExceptionally(error);
                case LoopEvent.SnapshotReceived received -> SnapshotFiles.deleteQuietly(received.payload());
                case LoopEvent.SnapshotGenerated generated -> SnapshotFiles.deleteQuietly(generated.payload());
                case LoopEvent.Step _, LoopEvent.SnapshotDelivered _, LoopEvent.Shutdown _ -> {
                }
            }
        }
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
