package io.partdb.raft.runtime;

import io.partdb.raft.LogEntry;
import io.partdb.raft.Membership;
import io.partdb.raft.Outcome;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

final class Requests<R> {

    sealed interface Pending<R> {
        CompletableFuture<?> future();

        record Proposal<R>(CompletableFuture<R> future) implements Pending<R> {}

        record Change<R>(CompletableFuture<Membership> future) implements Pending<R> {}

        record Read<R>(CompletableFuture<Long> future) implements Pending<R> {}

        record Transfer<R>(CompletableFuture<Void> future) implements Pending<R> {}
    }

    private record Commit<R>(long term, Pending<R> pending) {}

    private final Map<Long, Pending<R>> awaitingOutcome = new HashMap<>();
    private final NavigableMap<Long, Commit<R>> awaitingCommit = new TreeMap<>();
    private final NavigableMap<Long, List<CompletableFuture<Long>>> awaitingApply = new TreeMap<>();
    private long nextId;
    private long applied;

    Requests(long applied) {
        this.applied = applied;
    }

    long register(Pending<R> pending) {
        long id = ++nextId;
        awaitingOutcome.put(id, pending);
        return id;
    }

    long applied() {
        return applied;
    }

    void resolve(Outcome outcome) {
        var pending = awaitingOutcome.remove(outcome.id());
        if (pending == null) {
            return;
        }
        switch (outcome) {
            case Outcome.Appended(_, long index, long term) -> awaitingCommit.put(index, new Commit<>(term, pending));
            case Outcome.ReadReady(_, long index) -> {
                if (!(pending instanceof Pending.Read<R>(var future))) {
                    throw new IllegalStateException("read outcome for a " + pending.getClass().getSimpleName());
                }
                if (applied >= index) {
                    future.complete(index);
                } else {
                    awaitingApply.computeIfAbsent(index, _ -> new ArrayList<>()).add(future);
                }
            }
            case Outcome.Transferred _ -> {
                if (!(pending instanceof Pending.Transfer<R>(var future))) {
                    throw new IllegalStateException("transfer outcome for a " + pending.getClass().getSimpleName());
                }
                future.complete(null);
            }
            case Outcome.Rejected(_, var reason) -> pending.future().completeExceptionally(RaftException.of(reason));
        }
    }

    void applied(LogEntry entry, Optional<R> result) {
        var commit = awaitingCommit.remove(entry.index());
        if (commit != null) {
            boolean ours = commit.term() == entry.term();
            switch (commit.pending()) {
                case Pending.Proposal<R>(var future) when ours && result.isPresent() -> future.complete(result.get());
                case Pending.Change<R>(var future) when ours && entry instanceof LogEntry.Configuration configuration ->
                    future.complete(configuration.membership());
                case Pending<R> pending -> pending.future().completeExceptionally(new RaftException.NotLeader(Optional.empty()));
            }
        }
        advance(entry.index());
    }

    void superseded(long snapshotIndex) {
        var superseded = awaitingCommit.headMap(snapshotIndex, true);
        superseded.forEach((index, commit) -> commit.pending().future().completeExceptionally(new RaftException.OutcomeUnknown(index)));
        superseded.clear();
        advance(snapshotIndex);
    }

    void failAll(RuntimeException error) {
        awaitingOutcome.values().forEach(pending -> pending.future().completeExceptionally(error));
        awaitingOutcome.clear();
        awaitingCommit.values().forEach(commit -> commit.pending().future().completeExceptionally(error));
        awaitingCommit.clear();
        awaitingApply.values().forEach(futures -> futures.forEach(future -> future.completeExceptionally(error)));
        awaitingApply.clear();
    }

    private void advance(long index) {
        applied = Math.max(applied, index);
        var ready = awaitingApply.headMap(applied, true);
        ready.forEach((readIndex, futures) -> futures.forEach(future -> future.complete(readIndex)));
        ready.clear();
    }
}
