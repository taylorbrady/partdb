package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftEffects;
import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftInput;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftNode;
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
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
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
import java.util.stream.Stream;

public final class RaftRuntime implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RaftRuntime.class);
    private static final long BOOTSTRAP_TERM = 1;

    private sealed interface Event {
        record Proposal(long id, Bytes command) implements Event {}

        record Step(RaftInput input) implements Event {}

        record SnapshotReceived(String from, RaftMessage.InstallSnapshot request, Path payload) implements Event {}

        record Tick() implements Event {}

        record Shutdown() implements Event {}
    }

    private record PendingRpc(CompletableFuture<RaftMessage.Response> future, long createdAtTick) {}

    private record RpcKey(String peer, Class<? extends RaftMessage.Request> type) {}

    private record PendingCommit(long term, CompletableFuture<Bytes> future) {}

    private final String nodeId;
    private final RaftNode raft;
    private final long rpcTimeoutTicks;
    private final RaftTransport transport;
    private final FileRaftStorage storage;
    private final StateMachine stateMachine;

    private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
    private final Map<RpcKey, Deque<PendingRpc>> pendingRpcs = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Bytes>> pendingProposals = new ConcurrentHashMap<>();
    private final Map<Long, PendingCommit> pendingCommits = new ConcurrentHashMap<>();
    private final Map<Long, CompletableFuture<Long>> pendingReads = new ConcurrentHashMap<>();
    private final Set<String> snapshotTransfers = ConcurrentHashMap.newKeySet();
    private final ApplyTracker applyTracker = new ApplyTracker();
    private final AtomicLong proposalIds = new AtomicLong();
    private final AtomicLong readIds = new AtomicLong();

    private volatile long tickCount;
    private RaftRole lastRole;
    private String lastLeaderId;
    private Instant lastLeaderChange;
    private volatile RaftStatus status;
    private volatile RaftMembership membership;
    private volatile boolean running = true;

    private final ExecutorService ioExecutor = Executors.newVirtualThreadPerTaskExecutor();
    private final ScheduledExecutorService ticker;
    private final Thread eventLoop;

    private RaftRuntime(
        RaftConfig config,
        RaftNode raft,
        RaftTransport transport,
        FileRaftStorage storage,
        StateMachine stateMachine,
        long appliedIndex
    ) {
        this.nodeId = config.nodeId();
        this.raft = raft;
        this.rpcTimeoutTicks = config.options().electionTimeoutMax();
        this.transport = transport;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.lastRole = raft.role();
        this.lastLeaderId = raft.leaderId().orElse(null);
        applyTracker.advance(appliedIndex);
        publishStatus();

        this.ticker = Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().name("raft-ticker-", 0).factory());
        this.eventLoop = Thread.ofVirtual().name("raft-event-loop").start(this::runEventLoop);
        long tickMillis = config.tickInterval().toMillis();
        ticker.scheduleAtFixedRate(() -> events.offer(new Event.Tick()), tickMillis, tickMillis, TimeUnit.MILLISECONDS);
        transport.start(new Inbound());
    }

    public static RaftRuntime open(Path directory, RaftConfig config, RaftTransport transport, StateMachine stateMachine) {
        Objects.requireNonNull(directory, "directory must not be null");
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        Objects.requireNonNull(stateMachine, "stateMachine must not be null");

        FileRaftStorage storage = null;
        try {
            storage = FileRaftStorage.open(directory);
            var membership = storage.membership().orElse(config.membership());
            if (!membership.isMember(config.nodeId())) {
                throw new IllegalStateException("Recovered membership does not include " + config.nodeId());
            }

            var snapshot = storage.snapshot();
            long snapshotIndex = snapshot.map(file -> file.metadata().index()).orElse(0L);
            var raft = RaftNode.builder(config.nodeId(), membership, config.options(), storage)
                .electionJitter(bound -> ThreadLocalRandom.current().nextInt(bound))
                .restoredFrom(storage.hardState(), snapshotIndex)
                .build();

            if (snapshot.isPresent() && stateMachine.appliedIndex() < snapshotIndex) {
                restore(stateMachine, snapshot.get());
            }

            var replay = raft.step(new RaftInput.ReplayCommitted()).application();
            for (var entry : replay.entries()) {
                stateMachine.apply(entry.index(), entry.data());
            }
            if (replay.appliedThroughIndex() > 0) {
                raft.step(new RaftInput.Applied(replay.appliedThroughIndex()));
            }

            return new RaftRuntime(config, raft, transport, storage, stateMachine, raft.lastApplied());
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
                        throw new IllegalArgumentException("Raft directory must be empty: " + directory);
                    }
                }
            }
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare raft directory: " + directory, e);
        }

        if (index == 0) {
            return;
        }

        try (var storage = FileRaftStorage.open(directory)) {
            Path staged = storage.stagingFile();
            Files.copy(snapshot, staged, StandardCopyOption.REPLACE_EXISTING);
            storage.installSnapshot(new SnapshotMetadata(index, BOOTSTRAP_TERM, config.membership()), staged);
            storage.append(new RaftHardState(BOOTSTRAP_TERM, null, index), List.of());
            storage.sync();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to stage bootstrap snapshot: " + snapshot, e);
        }
    }

    public CompletableFuture<Bytes> propose(Bytes command) {
        Objects.requireNonNull(command, "command must not be null");
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Closed());
        }
        long id = proposalIds.incrementAndGet();
        var future = new CompletableFuture<Bytes>();
        pendingProposals.put(id, future);
        events.offer(new Event.Proposal(id, command));
        return future;
    }

    public CompletableFuture<Long> readBarrier() {
        if (!running) {
            return CompletableFuture.failedFuture(new RaftException.Closed());
        }
        long id = readIds.incrementAndGet();
        var future = new CompletableFuture<Long>();
        pendingReads.put(id, future);
        events.offer(new Event.Step(new RaftInput.ReadIndexRequested(id)));
        return future.thenCompose(applyTracker::waitFor);
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
        failPending(new RaftException.Closed());
        ticker.shutdown();
        transport.close();
        events.offer(new Event.Shutdown());

        try {
            eventLoop.join(TimeUnit.SECONDS.toMillis(5));
            ioExecutor.shutdown();
            ticker.awaitTermination(5, TimeUnit.SECONDS);
            ioExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        storage.close();
        publishStatus();
    }

    private void runEventLoop() {
        while (running) {
            try {
                switch (events.take()) {
                    case Event.Proposal(long id, Bytes command) -> handleProposal(id, command);
                    case Event.Step(RaftInput input) -> process(raft.step(input));
                    case Event.SnapshotReceived(var from, var request, var payload) -> handleSnapshot(from, request, payload);
                    case Event.Tick() -> {
                        tickCount++;
                        expireStaleRpcs();
                        process(raft.step(new RaftInput.Tick()));
                    }
                    case Event.Shutdown() -> {
                    }
                }
                publishStatus();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                log.atError()
                    .addKeyValue("nodeId", nodeId)
                    .setCause(e)
                    .log("Fatal raft runtime error, shutting down");
                running = false;
                failPending(new RaftException.StorageFailure(String.valueOf(e.getMessage()), e));
                publishStatus();
                return;
            }
        }
    }

    private void handleProposal(long id, Bytes command) {
        var future = pendingProposals.remove(id);
        if (future == null) {
            return;
        }
        if (!raft.isLeader()) {
            future.completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
            return;
        }

        var effects = raft.step(new RaftInput.EntryProposed(command));
        var entries = effects.persistence().entries();
        if (entries.isEmpty()) {
            future.completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
        } else {
            var proposed = entries.getLast();
            pendingCommits.put(proposed.index(), new PendingCommit(proposed.term(), future));
        }
        process(effects);
    }

    private void handleSnapshot(String from, RaftMessage.InstallSnapshot request, Path payload) {
        try {
            process(raft.step(new RaftInput.MessageReceived(from, request)), payload);
        } finally {
            try {
                Files.deleteIfExists(payload);
            } catch (IOException e) {
                log.atWarn().setCause(e).addKeyValue("payload", payload).log("Failed to delete staged snapshot");
            }
        }
    }

    private void process(RaftEffects effects) {
        process(effects, null);
    }

    private void process(RaftEffects effects, Path snapshotPayload) {
        if (!effects.hasWork()) {
            return;
        }
        observeLeadership();
        persist(effects.persistence(), snapshotPayload);
        for (var outbound : effects.messages()) {
            switch (outbound.message()) {
                case RaftMessage.Request request -> ioExecutor.execute(() -> sendRequest(outbound.to(), request));
                case RaftMessage.Response response -> completeRpc(outbound.to(), response);
            }
        }
        apply(effects.application());
        effects.snapshotNeeded().ifPresent(this::sendSnapshot);
    }

    private void persist(RaftEffects.Persistence persistence, Path snapshotPayload) {
        if (!persistence.hasWork()) {
            return;
        }

        storage.append(persistence.hardState().orElse(null), persistence.entries());
        persistence.incomingSnapshot().ifPresent(metadata -> {
            if (snapshotPayload == null) {
                throw new IllegalStateException("Raft accepted snapshot " + metadata.index() + " without a payload");
            }
            storage.installSnapshot(metadata, snapshotPayload);
            restore(stateMachine, storage.snapshot().orElseThrow());
            applyTracker.advance(metadata.index());
        });
        storage.sync();

        if (!persistence.entries().isEmpty()) {
            var last = persistence.entries().getLast();
            process(raft.step(new RaftInput.EntriesPersisted(last.index(), last.term())));
        }
        if (persistence.incomingSnapshot().isPresent()) {
            process(raft.step(new RaftInput.SnapshotPersisted()));
        }
    }

    private void apply(RaftEffects.Application application) {
        for (var entry : application.entries()) {
            Bytes result = stateMachine.apply(entry.index(), entry.data());
            var pending = pendingCommits.remove(entry.index());
            if (pending == null) {
                continue;
            }
            if (pending.term() == entry.term()) {
                pending.future().complete(result);
            } else {
                pending.future().completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
            }
        }

        if (application.appliedThroughIndex() > 0) {
            process(raft.step(new RaftInput.Applied(application.appliedThroughIndex())));
            applyTracker.advance(application.appliedThroughIndex());
        }

        for (var readState : application.readStates()) {
            var future = pendingReads.remove(readState.context());
            if (future == null) {
                continue;
            }
            if (readState.index() == 0) {
                future.completeExceptionally(new RaftException.NotLeader(raft.leaderId().orElse(null)));
            } else {
                future.complete(readState.index());
            }
        }
    }

    private void sendSnapshot(RaftEffects.SnapshotNeeded needed) {
        String peer = needed.peer();
        var snapshot = storage.snapshot();
        if (snapshot.isEmpty() || !snapshotTransfers.add(peer)) {
            return;
        }
        var request = new RaftMessage.InstallSnapshot(raft.term(), nodeId, snapshot.get().metadata());
        Path payload = snapshot.get().payload();
        ioExecutor.execute(() -> {
            try {
                var response = transport.sendSnapshot(peer, request, payload).join();
                events.offer(new Event.Step(new RaftInput.MessageReceived(peer, response)));
            } catch (RuntimeException e) {
                log.atDebug()
                    .addKeyValue("nodeId", nodeId)
                    .addKeyValue("peer", peer)
                    .addKeyValue("error", e.getMessage())
                    .log("Failed to send snapshot");
            } finally {
                snapshotTransfers.remove(peer);
            }
        });
    }

    private void sendRequest(String to, RaftMessage.Request request) {
        try {
            var response = transport.send(to, request).join();
            events.offer(new Event.Step(new RaftInput.MessageReceived(to, response)));
        } catch (RuntimeException e) {
            log.atDebug()
                .addKeyValue("nodeId", nodeId)
                .addKeyValue("peer", to)
                .addKeyValue("error", e.getMessage())
                .log("Failed to send request");
        }
    }

    private CompletableFuture<RaftMessage.Response> awaitResponse(String from, Class<? extends RaftMessage.Request> type) {
        var future = new CompletableFuture<RaftMessage.Response>();
        pendingRpcs.computeIfAbsent(new RpcKey(from, type), _ -> new ConcurrentLinkedDeque<>())
            .addLast(new PendingRpc(future, tickCount));
        return future;
    }

    private void completeRpc(String to, RaftMessage.Response response) {
        Class<? extends RaftMessage.Request> type = switch (response) {
            case RaftMessage.AppendEntriesResponse _ -> RaftMessage.AppendEntries.class;
            case RaftMessage.RequestVoteResponse _ -> RaftMessage.RequestVote.class;
            case RaftMessage.InstallSnapshotResponse _ -> RaftMessage.InstallSnapshot.class;
            case RaftMessage.PreVoteResponse _ -> RaftMessage.PreVote.class;
            case RaftMessage.ReadIndexResponse _ -> RaftMessage.ReadIndexRequested.class;
        };
        var key = new RpcKey(to, type);
        var queue = pendingRpcs.get(key);
        if (queue == null) {
            return;
        }
        var pending = queue.pollFirst();
        if (pending != null) {
            pending.future().complete(response);
        }
        if (queue.isEmpty()) {
            pendingRpcs.remove(key);
        }
    }

    private void expireStaleRpcs() {
        long threshold = tickCount - rpcTimeoutTicks;
        pendingRpcs.entrySet().removeIf(entry -> {
            var queue = entry.getValue();
            PendingRpc pending;
            while ((pending = queue.peekFirst()) != null && pending.createdAtTick() < threshold) {
                queue.pollFirst();
                pending.future().completeExceptionally(new RaftException.RpcTimeout(entry.getKey().peer()));
            }
            return queue.isEmpty();
        });
    }

    private void observeLeadership() {
        RaftRole role = raft.role();
        String leaderId = raft.leaderId().orElse(null);
        if (role == lastRole && Objects.equals(leaderId, lastLeaderId)) {
            return;
        }
        log.atInfo()
            .addKeyValue("nodeId", nodeId)
            .addKeyValue("term", raft.term())
            .addKeyValue("previousRole", lastRole)
            .addKeyValue("newRole", role)
            .log("State transition");

        var event = new LeaderChangeEvent();
        event.nodeId = nodeId;
        event.term = raft.term();
        event.previousRole = lastRole.name();
        event.newRole = role.name();
        event.previousLeaderId = lastLeaderId;
        event.newLeaderId = leaderId;
        event.commit();

        lastRole = role;
        lastLeaderId = leaderId;
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
            applyTracker.lastApplied(),
            Optional.ofNullable(lastLeaderChange),
            running
        );
    }

    private void failPending(Throwable cause) {
        pendingProposals.values().forEach(future -> future.completeExceptionally(cause));
        pendingProposals.clear();
        pendingCommits.values().forEach(pending -> pending.future().completeExceptionally(cause));
        pendingCommits.clear();
        pendingReads.values().forEach(future -> future.completeExceptionally(cause));
        pendingReads.clear();
        pendingRpcs.values().forEach(queue -> queue.forEach(pending -> pending.future().completeExceptionally(cause)));
        pendingRpcs.clear();
        applyTracker.failAll(cause);
    }

    private static void restore(StateMachine stateMachine, SnapshotStore.SnapshotFile snapshot) {
        try (InputStream in = new BufferedInputStream(Files.newInputStream(snapshot.payload()))) {
            stateMachine.restore(snapshot.metadata().index(), in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to restore snapshot " + snapshot.metadata().index(), e);
        }
    }

    private final class Inbound implements RaftTransport.Inbound {
        @Override
        public CompletableFuture<RaftMessage.Response> receive(String from, RaftMessage.Request request) {
            if (!running) {
                return CompletableFuture.failedFuture(new RaftException.Closed());
            }
            var response = awaitResponse(from, request.getClass());
            events.offer(new Event.Step(new RaftInput.MessageReceived(from, request)));
            return response;
        }

        @Override
        public RaftTransport.SnapshotReceiver receiveSnapshot(String from, RaftMessage.InstallSnapshot request) throws IOException {
            if (!running) {
                throw new RaftException.Closed();
            }
            return new StagedSnapshot(from, request, storage.stagingFile());
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
        public CompletableFuture<RaftMessage.InstallSnapshotResponse> complete() throws IOException {
            channel.force(true);
            channel.close();
            if (!running) {
                Files.deleteIfExists(path);
                return CompletableFuture.failedFuture(new RaftException.Closed());
            }
            var response = awaitResponse(from, RaftMessage.InstallSnapshot.class);
            events.offer(new Event.SnapshotReceived(from, request, path));
            return response.thenApply(RaftMessage.InstallSnapshotResponse.class::cast);
        }

        @Override
        public void abort() {
            try {
                channel.close();
                Files.deleteIfExists(path);
            } catch (IOException e) {
                log.atWarn().setCause(e).addKeyValue("payload", path).log("Failed to discard staged snapshot");
            }
        }
    }
}
