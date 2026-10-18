package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.raft.Membership;
import io.partdb.raft.runtime.RaftRuntime;
import io.partdb.raft.runtime.RaftStatus;
import io.partdb.raft.runtime.RaftTransport;
import io.partdb.storage.Cursor;
import io.partdb.storage.KeyRange;
import io.partdb.storage.KeyValue;
import io.partdb.storage.Mutation;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

public final class Node implements AutoCloseable {

    private final KvStateMachine stateMachine;
    private final RaftRuntime<TxnResult> raft;
    private final KvLimits limits;
    private final AtomicLong proposals = new AtomicLong();
    private final AtomicLong failedProposals = new AtomicLong();

    private Node(KvStateMachine stateMachine, RaftRuntime<TxnResult> raft, KvLimits limits) {
        this.stateMachine = stateMachine;
        this.raft = raft;
        this.limits = limits;
    }

    public static Node open(NodeConfig config, RaftTransport transport) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");
        var stateMachine = KvStateMachine.open(config.dataDirectory().resolve("db"));
        try {
            return new Node(stateMachine, RaftRuntime.open(config.dataDirectory().resolve("raft"), config.raft(), transport, stateMachine), config.limits());
        } catch (RuntimeException e) {
            stateMachine.close();
            throw e;
        }
    }

    public Read<Optional<KeyValue>> get(Bytes key, ReadConsistency consistency, Duration timeout) {
        Objects.requireNonNull(key, "key must not be null");
        valid(() -> limits.checkKey(key));
        barrier(consistency, timeout);
        return stateMachine.get(key);
    }

    public Cursor scan(KeyRange range, ReadConsistency consistency, Duration timeout) {
        Objects.requireNonNull(range, "range must not be null");
        barrier(consistency, timeout);
        return stateMachine.scan(range);
    }

    public long write(List<Mutation> mutations, Duration timeout) {
        return switch (transact(valid(() -> Txn.of(mutations)), timeout)) {
            case TxnResult.Committed(long revision) -> revision;
            case TxnResult.ConditionFailed() -> throw new IllegalStateException("an unconditional transaction failed a condition");
        };
    }

    public TxnResult transact(Txn txn, Duration timeout) {
        Objects.requireNonNull(txn, "txn must not be null");
        valid(() -> limits.check(txn));
        proposals.incrementAndGet();
        try {
            return await(raft.propose(TxnCodec.encode(txn)), timeout,
                waited -> new NodeException.OutcomeUnknown("the write did not complete within " + waited, null));
        } catch (NodeException e) {
            failedProposals.incrementAndGet();
            throw e;
        }
    }

    public RaftStatus status() {
        return raft.status();
    }

    public Membership membership() {
        return raft.membership();
    }

    public void stepDown(Duration timeout) {
        await(raft.stepDown(), timeout, waited -> new NodeException.Timeout("leadership did not move within " + waited));
    }

    public NodeMetrics metrics() {
        return new NodeMetrics(proposals.get(), failedProposals.get(), stateMachine.stats());
    }

    @Override
    public void close() {
        try {
            raft.close();
        } finally {
            stateMachine.close();
        }
    }

    private void barrier(ReadConsistency consistency, Duration timeout) {
        Objects.requireNonNull(consistency, "consistency must not be null");
        switch (consistency) {
            case LINEARIZABLE -> await(raft.readBarrier(), timeout, waited -> new NodeException.Timeout("the read did not complete within " + waited));
            case LOCAL -> {}
        }
    }

    private static <T> T valid(Supplier<T> value) {
        try {
            return value.get();
        } catch (IllegalArgumentException e) {
            throw new NodeException.InvalidRequest(e.getMessage(), e);
        }
    }

    private static <T> T await(CompletionStage<T> stage, Duration timeout, Function<Duration, NodeException> timedOut) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        var future = stage.toCompletableFuture();
        try {
            return future.get(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            future.cancel(false);
            throw timedOut.apply(timeout);
        } catch (ExecutionException e) {
            throw NodeException.of(e);
        } catch (InterruptedException e) {
            future.cancel(false);
            Thread.currentThread().interrupt();
            throw new NodeException.Cancelled("interrupted while waiting for raft");
        }
    }
}
