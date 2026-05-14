package io.partdb.consensus;

import io.partdb.bytes.Bytes;
import io.partdb.raft.RaftEffects;
import io.partdb.raft.RaftInput;
import io.partdb.raft.RaftMessage;
import io.partdb.raft.RaftNode;
import io.partdb.raft.RaftSnapshot;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

final class RaftEffectRunner {
    private final RaftNode raft;
    private final RaftStorage storage;
    private final ReplicatedStateMachine stateMachine;
    private final ConcurrentHashMap<Long, CompletableFuture<ProposalResult>> pendingCommits;
    private final ConcurrentHashMap<Long, CompletableFuture<ReadBarrier>> pendingReads;
    private final ApplyTracker applyTracker;
    private final ExecutorService ioExecutor;
    private final Runnable observeLeadership;
    private final Consumer<RaftEffects.Outbound> sendRequest;
    private final Consumer<RaftEffects.Outbound> completeRpc;
    private final Consumer<RaftEffects.SnapshotNeeded> sendSnapshot;

    RaftEffectRunner(
        RaftNode raft,
        RaftStorage storage,
        ReplicatedStateMachine stateMachine,
        ConcurrentHashMap<Long, CompletableFuture<ProposalResult>> pendingCommits,
        ConcurrentHashMap<Long, CompletableFuture<ReadBarrier>> pendingReads,
        ApplyTracker applyTracker,
        ExecutorService ioExecutor,
        Runnable observeLeadership,
        Consumer<RaftEffects.Outbound> sendRequest,
        Consumer<RaftEffects.Outbound> completeRpc,
        Consumer<RaftEffects.SnapshotNeeded> sendSnapshot
    ) {
        this.raft = raft;
        this.storage = storage;
        this.stateMachine = stateMachine;
        this.pendingCommits = pendingCommits;
        this.pendingReads = pendingReads;
        this.applyTracker = applyTracker;
        this.ioExecutor = ioExecutor;
        this.observeLeadership = observeLeadership;
        this.sendRequest = sendRequest;
        this.completeRpc = completeRpc;
        this.sendSnapshot = sendSnapshot;
    }

    void process(RaftEffects effects) {
        if (!effects.hasWork()) {
            return;
        }

        observeLeadership.run();
        persist(effects.persistence());
        dispatchMessages(effects);
        apply(effects.application());
        effects.snapshotNeeded().ifPresent(transfer -> ioExecutor.execute(() -> sendSnapshot.accept(transfer)));
    }

    private void persist(RaftEffects.Persistence persistence) {
        if (!persistence.hasWork()) {
            return;
        }

        storage.append(persistence.hardState().orElse(null), persistence.entries());

        persistence.incomingSnapshot().ifPresent(snapshot -> {
            storage.saveSnapshot(snapshot);
            stateMachine.restore(StoredSnapshot.fromRaftSnapshot(snapshot));
            applyTracker.advance(snapshot.index());
        });

        if (persistence.requiresSync()) {
            storage.sync();
        }

        if (!persistence.entries().isEmpty()) {
            var lastPersisted = persistence.entries().getLast();
            process(raft.step(new RaftInput.EntriesPersisted(lastPersisted.index(), lastPersisted.term())));
        }
        persistence.incomingSnapshot().ifPresent(_ ->
            process(raft.step(new RaftInput.SnapshotPersisted()))
        );
    }

    private void dispatchMessages(RaftEffects effects) {
        for (var outbound : effects.messages()) {
            switch (outbound.message()) {
                case RaftMessage.Request _ -> ioExecutor.execute(() -> sendRequest.accept(outbound));
                case RaftMessage.Response _ -> completeRpc.accept(outbound);
            }
        }
    }

    private void apply(RaftEffects.Application application) {
        for (var entry : application.entries()) {
            StateMachineResult result = stateMachine.apply(new CommittedCommand(entry.index(), entry.term(), entry.data()));
            var commitFuture = pendingCommits.remove(entry.index());
            if (commitFuture != null) {
                switch (result) {
                    case StateMachineResult.Applied(var output) ->
                        commitFuture.complete(new ProposalResult.Applied(entry.index(), entry.term(), output));
                    case StateMachineResult.Rejected(var output) ->
                        commitFuture.complete(new ProposalResult.Rejected(entry.index(), entry.term(), output));
                }
            }
        }

        long appliedThroughIndex = application.appliedThroughIndex();
        if (appliedThroughIndex > 0) {
            process(raft.step(new RaftInput.Applied(appliedThroughIndex)));
            applyTracker.advance(appliedThroughIndex);
        }

        for (var readState : application.readStates()) {
            long id = bytesToLong(readState.context());
            var future = pendingReads.remove(id);
            if (future != null) {
                if (readState.index() == 0) {
                    future.completeExceptionally(new ConsensusException.NotLeader(raft.leaderId().orElse(null)));
                } else {
                    future.complete(new ReadBarrier(readState.index(), raft.term()));
                }
            }
        }
    }

    private static long bytesToLong(Bytes bytes) {
        return ByteBuffer.wrap(bytes.toByteArray()).getLong();
    }
}
