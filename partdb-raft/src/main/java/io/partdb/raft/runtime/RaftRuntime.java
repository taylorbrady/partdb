package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Envelope;
import io.partdb.raft.MembershipChange;
import io.partdb.raft.Raft;
import io.partdb.raft.RaftEffects;
import io.partdb.raft.RaftEffects.ProposalResult;
import io.partdb.raft.RaftEffects.ReadResult;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftInput;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftRole;
import io.partdb.raft.SnapshotMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

public final class RaftRuntime implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RaftRuntime.class);
    private static final Executor CALLBACKS = Thread::startVirtualThread;
    private static final long BOOTSTRAP_TERM = 1;
    private static final int MAX_BATCH = 4096;

    private sealed interface Event {
        record Input(RaftInput input) implements Event {}

        record Propose(Bytes command, CompletableFuture<Bytes> future) implements Event {}

        record Read(CompletableFuture<Long> future) implements Event {}

        record ChangeMembership(MembershipChange change, CompletableFuture<RaftMembership> future) implements Event {}

        record Transfer(String target, CompletableFuture<Void> future) implements Event {}

        record SnapshotReceived(String from, RaftMessage.InstallSnapshot request, Path payload) implements Event {}

        record SnapshotDelivered(String peer, boolean delivered) implements Event {}

        record Shutdown() implements Event {}
    }

    private sealed interface PendingCommit {
        long term();

        void fail(RuntimeException error);
    }

    private record PendingCommand(long term, CompletableFuture<Bytes> future) implements PendingCommit {
        @Override
        public void fail(RuntimeException error) {
            future.completeExceptionally(error);
        }
    }

    private record PendingMembership(long term, CompletableFuture<RaftMembership> future) implements PendingCommit {
        @Override
        public void fail(RuntimeException error) {
            future.completeExceptionally(error);
        }
    }

    private record PendingTransfer(String target, long deadline, CompletableFuture<Void> future) {}

    private final String nodeId;
    private final long tickNanos;
    private final long transferTimeoutNanos;
    private final Raft raft;
    private final FileRaftStorage storage;
    private final RaftTransport transport;
    private final StateMachine stateMachine;
    private final ApplyTracker applied = new ApplyTracker();
    private final LinkedBlockingQueue<Event> events = new LinkedBlockingQueue<>();

    private final Map<Long, CompletableFuture<Bytes>> proposals = new HashMap<>();
    private final Map<Long, CompletableFuture<RaftMembership>> membershipChanges = new HashMap<>();
    private final Map<Long, CompletableFuture<Long>> reads = new HashMap<>();
    private final NavigableMap<Long, PendingCommit> commits = new TreeMap<>();
    private final List<PendingTransfer> transfers = new ArrayList<>();
    private final Map<Long, Path> stagedSnapshots = new HashMap<>();
    private final Set<String> snapshotTransfers = new HashSet<>();

    private long requestIds;
    private RaftHardState syncedState;
    private RaftRole lastRole;
    private Optional<String> lastLeader;
    private Instant lastLeaderChange;
    private volatile boolean running = true;
    private volatile RaftStatus status;
    private volatile RaftMembership membership;
    private Thread loop;

    private RaftRuntime(RaftConfig config, Raft raft, FileRaftStorage storage, RaftTransport transport, StateMachine stateMachine) {
        this.nodeId = config.nodeId();
        this.tickNanos = config.tickInterval().toNanos();
        this.transferTimeoutNanos = 2L * config.options().electionTicks() * tickNanos;
        this.raft = raft;
        this.storage = storage;
        this.transport = transport;
        this.stateMachine = stateMachine;
        this.syncedState = storage.hardState();
        this.lastRole = raft.role();
        this.lastLeader = raft.leaderId();
        applied.advance(raft.appliedIndex());
    }

    public static RaftRuntime open(Path directory, RaftConfig config, RaftTransport transport, StateMachine stateMachine) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        FileRaftStorage storage = null;
        try {
            storage = FileRaftStorage.open(directory);
            var snapshot = storage.snapshot();
            long snapshotIndex = snapshot.map(file -> file.metadata().index()).orElse(0L);
            if (snapshot.isPresent() && stateMachine.appliedIndex() < snapshotIndex) {
                restore(stateMachine, snapshot.get());
            }
            var raft = Raft.recover(
                config.nodeId(),
                snapshot.map(file -> file.metadata().membership()).orElse(config.initialMembership()),
                config.options(),
                storage,
                storage.hardState(),
                Math.max(stateMachine.appliedIndex(), snapshotIndex),
                new SplittableRandom()
            );
            var runtime = new RaftRuntime(config, raft, storage, transport, stateMachine);
            runtime.processReady();
            runtime.publishStatus();
            runtime.start();
            return runtime;
        } catch (RuntimeException e) {
            if (storage != null) {
                storage.close();
            }
            transport.close();
            throw e;
        }
    }

    public static void bootstrap(Path directory, RaftConfig config, long index, Path snapshot) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        if (index < 0) {
            throw new IllegalArgumentException("index must not be negative");
        }
        try {
            if (Files.exists(directory)) {
                try (Stream<Path> entries = Files.list(directory)) {
                    if (entries.findAny().isPresent()) {
                        throw new IllegalArgumentException("raft directory must be empty: " + directory);
                    }
                }
            }
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to prepare raft directory " + directory, e);
        }
        if (index == 0) {
            return;
        }
        try (var storage = FileRaftStorage.open(directory)) {
            Path staged = storage.stagingFile();
            Files.copy(snapshot, staged, StandardCopyOption.REPLACE_EXISTING);
            storage.installSnapshot(new SnapshotMetadata(index, BOOTSTRAP_TERM, config.initialMembership()), staged);
            storage.append(Optional.of(new RaftHardState(BOOTSTRAP_TERM, Optional.empty(), index)), List.of());
            storage.sync();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to stage bootstrap snapshot " + snapshot, e);
        }
    }

    public CompletionStage<Bytes> propose(Bytes command) {
        Objects.requireNonNull(command, "command must not be null");
        var future = new CompletableFuture<Bytes>();
        return submit(new Event.Propose(command, future), future);
    }

    public CompletionStage<Long> readBarrier() {
        var future = new CompletableFuture<Long>();
        return submit(new Event.Read(future), future);
    }

    public CompletionStage<RaftMembership> changeMembership(MembershipChange change) {
        Objects.requireNonNull(change, "change must not be null");
        var future = new CompletableFuture<RaftMembership>();
        return submit(new Event.ChangeMembership(change, future), future);
    }

    public CompletionStage<Void> transferLeadership(String target) {
        Objects.requireNonNull(target, "target must not be null");
        var future = new CompletableFuture<Void>();
        return submit(new Event.Transfer(target, future), future);
    }

    public RaftStatus status() {
        return status;
    }

    public RaftMembership membership() {
        return membership;
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        events.add(new Event.Shutdown());
        try {
            loop.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        transport.close();
        failPending(new RaftException.Closed());
        stagedSnapshots.values().forEach(RaftRuntime::deleteQuietly);
        storage.close();
        publishStatus();
    }

    private <T> CompletionStage<T> submit(Event event, CompletableFuture<T> future) {
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Closed());
        }
        events.add(event);
        if (!running) {
            future.completeExceptionally(new RaftException.Closed());
        }
        return future.thenApplyAsync(Function.identity(), CALLBACKS);
    }

    private void start() {
        loop = Thread.ofVirtual().name("raft-" + nodeId).start(this::run);
        transport.start(new Receiver());
    }

    private void run() {
        var batch = new ArrayList<Event>();
        long nextTick = System.nanoTime() + tickNanos;
        try {
            while (running) {
                long wait = nextTick - System.nanoTime();
                var event = wait > 0 ? events.poll(wait, TimeUnit.NANOSECONDS) : events.poll();
                if (event != null) {
                    batch.add(event);
                    events.drainTo(batch, MAX_BATCH);
                }
                for (Event next : batch) {
                    handle(next);
                }
                batch.clear();
                long now = System.nanoTime();
                if (now - nextTick >= 0) {
                    raft.step(new RaftInput.Tick());
                    nextTick = now + tickNanos;
                }
                processReady();
                discardUnusedSnapshots();
                resolveTransfers(now);
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

    private void handle(Event event) {
        switch (event) {
            case Event.Input(var input) -> raft.step(input);
            case Event.Propose(var command, var future) -> {
                long id = ++requestIds;
                proposals.put(id, future);
                raft.step(new RaftInput.Propose(id, command));
            }
            case Event.Read(var future) -> {
                long id = ++requestIds;
                reads.put(id, future);
                raft.step(new RaftInput.ReadIndex(id));
            }
            case Event.ChangeMembership(var change, var future) -> {
                long id = ++requestIds;
                membershipChanges.put(id, future);
                raft.step(new RaftInput.ChangeMembership(id, change));
            }
            case Event.Transfer(var target, var future) -> transfer(target, future);
            case Event.SnapshotReceived(var from, var request, var payload) -> {
                var previous = stagedSnapshots.put(request.snapshot().index(), payload);
                if (previous != null) {
                    deleteQuietly(previous);
                }
                raft.step(new RaftInput.Receive(from, request));
            }
            case Event.SnapshotDelivered(var peer, boolean delivered) -> {
                snapshotTransfers.remove(peer);
                raft.step(delivered ? new RaftInput.SnapshotSent(peer) : new RaftInput.SnapshotFailed(peer));
            }
            case Event.Shutdown() -> {
            }
        }
    }

    private void processReady() {
        for (var effects = raft.ready(); !effects.isEmpty(); effects = raft.ready()) {
            effects.snapshot().ifPresent(this::installSnapshot);
            persist(effects);
            effects.messages().forEach(transport::send);
            effects.proposals().forEach(this::resolveProposal);
            for (RaftLogEntry entry : effects.committed()) {
                apply(entry);
            }
            if (!effects.committed().isEmpty()) {
                applied.advance(effects.committed().getLast().index());
            }
            effects.reads().forEach(this::resolveRead);
            effects.snapshotRequests().forEach(this::sendSnapshot);
            effects.persisted().ifPresent(raft::step);
        }
    }

    private void persist(RaftEffects effects) {
        if (effects.entries().isEmpty() && effects.hardState().isEmpty()) {
            return;
        }
        storage.append(effects.hardState(), effects.entries());
        var state = effects.hardState().orElse(syncedState);
        boolean voteChanged = state.term() != syncedState.term() || !state.votedFor().equals(syncedState.votedFor());
        if (!effects.entries().isEmpty() || voteChanged) {
            storage.sync();
        }
        syncedState = state;
    }

    private void installSnapshot(SnapshotMetadata metadata) {
        Path staged = stagedSnapshots.remove(metadata.index());
        if (staged == null) {
            throw new IllegalStateException("raft accepted snapshot " + metadata.index() + " without a payload");
        }
        storage.installSnapshot(metadata, staged);
        restore(stateMachine, storage.snapshot().orElseThrow());
        var superseded = commits.headMap(metadata.index(), true);
        superseded.forEach((index, pending) -> pending.fail(new RaftException.OutcomeUnknown(index)));
        superseded.clear();
        applied.advance(metadata.index());
    }

    private void resolveProposal(ProposalResult result) {
        switch (result) {
            case ProposalResult.Accepted(long id, long index, long term) -> {
                var proposal = proposals.remove(id);
                if (proposal != null) {
                    commits.put(index, new PendingCommand(term, proposal));
                }
                var change = membershipChanges.remove(id);
                if (change != null) {
                    commits.put(index, new PendingMembership(term, change));
                }
            }
            case ProposalResult.Rejected(long id) -> {
                var error = notLeader();
                var proposal = proposals.remove(id);
                if (proposal != null) {
                    proposal.completeExceptionally(error);
                }
                var change = membershipChanges.remove(id);
                if (change != null) {
                    change.completeExceptionally(error);
                }
            }
        }
    }

    private void apply(RaftLogEntry entry) {
        var pending = commits.remove(entry.index());
        boolean ours = pending != null && pending.term() == entry.term();
        switch (entry) {
            case RaftLogEntry.Command command -> {
                Bytes result = stateMachine.apply(command.index(), command.command());
                if (ours && pending instanceof PendingCommand(_, var future)) {
                    future.complete(result);
                    return;
                }
            }
            case RaftLogEntry.Membership change -> {
                if (ours && pending instanceof PendingMembership(_, var future)) {
                    future.complete(change.membership());
                    return;
                }
            }
            case RaftLogEntry.NoOp _ -> {
            }
        }
        if (pending != null) {
            pending.fail(notLeader());
        }
    }

    private void resolveRead(ReadResult result) {
        var future = reads.remove(result.id());
        if (future == null) {
            return;
        }
        switch (result) {
            case ReadResult.Confirmed(_, long index) -> applied.waitFor(index).whenComplete((value, error) -> {
                if (error != null) {
                    future.completeExceptionally(error);
                } else {
                    future.complete(value);
                }
            });
            case ReadResult.Rejected _ -> future.completeExceptionally(notLeader());
        }
    }

    private void transfer(String target, CompletableFuture<Void> future) {
        if (raft.role() != RaftRole.LEADER) {
            future.completeExceptionally(notLeader());
        } else if (!raft.membership().isVoter(target)) {
            future.completeExceptionally(new IllegalArgumentException(target + " is not a voter"));
        } else if (target.equals(nodeId)) {
            future.complete(null);
        } else {
            raft.step(new RaftInput.TransferLeadership(target));
            transfers.add(new PendingTransfer(target, System.nanoTime() + transferTimeoutNanos, future));
        }
    }

    private void resolveTransfers(long now) {
        var iterator = transfers.iterator();
        while (iterator.hasNext()) {
            var transfer = iterator.next();
            if (raft.leaderId().filter(transfer.target()::equals).isPresent()) {
                transfer.future().complete(null);
                iterator.remove();
            } else if (now - transfer.deadline() >= 0) {
                transfer.future().completeExceptionally(new RaftException.TransferTimedOut(transfer.target()));
                iterator.remove();
            }
        }
    }

    private void sendSnapshot(String peer) {
        var snapshot = storage.snapshot().orElseThrow(() -> new IllegalStateException("raft requested a snapshot that does not exist"));
        if (!snapshotTransfers.add(peer)) {
            return;
        }
        var request = new RaftMessage.InstallSnapshot(raft.term(), snapshot.metadata());
        transport.sendSnapshot(peer, request, snapshot.payload()).whenComplete((_, error) -> {
            if (error != null) {
                log.atDebug().addKeyValue("nodeId", nodeId).addKeyValue("peer", peer).setCause(error).log("Snapshot transfer failed");
            }
            events.add(new Event.SnapshotDelivered(peer, error == null));
        });
    }

    private void discardUnusedSnapshots() {
        stagedSnapshots.values().forEach(RaftRuntime::deleteQuietly);
        stagedSnapshots.clear();
    }

    private RaftException.NotLeader notLeader() {
        return new RaftException.NotLeader(raft.leaderId());
    }

    private void observeLeadership() {
        RaftRole role = raft.role();
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
        event.nodeId = nodeId;
        event.term = raft.term();
        event.previousRole = lastRole.name();
        event.newRole = role.name();
        event.previousLeaderId = lastLeader.orElse(null);
        event.newLeaderId = leader.orElse(null);
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
            applied.lastApplied(),
            Optional.ofNullable(lastLeaderChange),
            running
        );
    }

    private void failPending(RuntimeException error) {
        proposals.values().forEach(future -> future.completeExceptionally(error));
        proposals.clear();
        membershipChanges.values().forEach(future -> future.completeExceptionally(error));
        membershipChanges.clear();
        reads.values().forEach(future -> future.completeExceptionally(error));
        reads.clear();
        commits.values().forEach(pending -> pending.fail(error));
        commits.clear();
        transfers.forEach(transfer -> transfer.future().completeExceptionally(error));
        transfers.clear();
        applied.failAll(error);
        for (Event event; (event = events.poll()) != null; ) {
            switch (event) {
                case Event.Propose(_, var future) -> future.completeExceptionally(error);
                case Event.Read(var future) -> future.completeExceptionally(error);
                case Event.ChangeMembership(_, var future) -> future.completeExceptionally(error);
                case Event.Transfer(_, var future) -> future.completeExceptionally(error);
                case Event.SnapshotReceived(_, _, var payload) -> deleteQuietly(payload);
                case Event.Input _, Event.SnapshotDelivered _, Event.Shutdown _ -> {
                }
            }
        }
    }

    private static void restore(StateMachine stateMachine, SnapshotStore.SnapshotFile snapshot) {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(snapshot.payload()))) {
            stateMachine.restore(snapshot.metadata().index(), in);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to restore snapshot " + snapshot.metadata().index(), e);
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.atWarn().addKeyValue("path", path).setCause(e).log("Failed to delete staged snapshot");
        }
    }

    private final class Receiver implements RaftTransport.Receiver {
        @Override
        public void receive(Envelope envelope) {
            if (running && !(envelope.message() instanceof RaftMessage.InstallSnapshot)) {
                events.add(new Event.Input(new RaftInput.Receive(envelope.from(), envelope.message())));
            }
        }

        @Override
        public RaftTransport.SnapshotReceiver receiveSnapshot(String from, RaftMessage.InstallSnapshot request) throws IOException {
            if (!running) {
                throw new RaftException.Closed();
            }
            return new StagedSnapshot(from, request, storage.stagingFile());
        }

        @Override
        public void unreachable(String peer) {
            if (running) {
                events.add(new Event.Input(new RaftInput.Unreachable(peer)));
            }
        }
    }

    private final class StagedSnapshot implements RaftTransport.SnapshotReceiver {
        private final String from;
        private final RaftMessage.InstallSnapshot request;
        private final Path path;
        private final FileChannel channel;

        private StagedSnapshot(String from, RaftMessage.InstallSnapshot request, Path path) throws IOException {
            this.from = from;
            this.request = request;
            this.path = path;
            this.channel = FileChannel.open(path, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        @Override
        public void write(ByteBuffer chunk) throws IOException {
            while (chunk.hasRemaining()) {
                channel.write(chunk);
            }
        }

        @Override
        public void complete() throws IOException {
            channel.force(true);
            channel.close();
            if (!running) {
                deleteQuietly(path);
                throw new RaftException.Closed();
            }
            events.add(new Event.SnapshotReceived(from, request, path));
        }

        @Override
        public void abort() {
            try {
                channel.close();
            } catch (IOException e) {
                log.atDebug().setCause(e).log("Failed to close staged snapshot");
            }
            deleteQuietly(path);
        }
    }
}
