package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValue;
import io.partdb.node.kv.KeyValueStore;
import io.partdb.node.kv.KvLimits;
import io.partdb.node.kv.Read;
import io.partdb.node.kv.ReadConsistency;
import io.partdb.node.kv.Scan;
import io.partdb.node.kv.Txn;
import io.partdb.node.kv.TxnResult;
import io.partdb.node.state.KvStateMachine;
import io.partdb.node.state.TxnCodec;
import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;
import io.partdb.raft.runtime.RaftRuntime;
import io.partdb.raft.runtime.RaftStatus;
import io.partdb.raft.runtime.RaftTransport;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public final class Node implements AutoCloseable {

    private enum Kind { READ, WRITE }

    private final KvStateMachine stateMachine;
    private final RaftRuntime<TxnResult> raft;
    private final KvLimits limits;
    private final AtomicLong proposals = new AtomicLong();
    private final AtomicLong failedProposals = new AtomicLong();
    private final KeyValueStore kv = new KeyValues();
    private final ClusterView cluster = new Cluster();

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

    public static Node standalone(NodeConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        if (config.raft().initialMembership().members().size() != 1) {
            throw new IllegalArgumentException("a node without a transport must be the only cluster member");
        }
        return open(config, RaftTransport.standalone());
    }

    public KeyValueStore kv() {
        return kv;
    }

    public ClusterView cluster() {
        return cluster;
    }

    public NodeMetrics metrics() {
        return new NodeMetrics(proposals.get(), failedProposals.get(), stateMachine.metrics());
    }

    @Override
    public void close() {
        try {
            raft.close();
        } finally {
            stateMachine.close();
        }
    }

    private <T> T read(ReadConsistency consistency, Duration timeout, Supplier<T> read) {
        Objects.requireNonNull(consistency, "consistency must not be null");
        if (consistency == ReadConsistency.LINEARIZABLE) {
            await(raft.readBarrier(), timeout, Kind.READ);
        }
        return read.get();
    }

    private static <T> T await(CompletionStage<T> stage, Duration timeout, Kind kind) {
        Objects.requireNonNull(timeout, "timeout must not be null");
        var future = stage.toCompletableFuture();
        try {
            return future.get(Math.max(0, timeout.toNanos()), TimeUnit.NANOSECONDS);
        } catch (TimeoutException e) {
            future.cancel(false);
            throw switch (kind) {
                case READ -> new NodeException.Timeout("the read did not complete within " + timeout);
                case WRITE -> new NodeException.OutcomeUnknown("the write did not complete within " + timeout, e);
            };
        } catch (ExecutionException e) {
            throw NodeException.of(e);
        } catch (InterruptedException e) {
            future.cancel(false);
            Thread.currentThread().interrupt();
            throw new NodeException.Cancelled("interrupted while waiting for raft");
        }
    }

    private final class KeyValues implements KeyValueStore {
        @Override
        public Read<Optional<KeyValue>> get(Bytes key, ReadConsistency consistency, Duration timeout) {
            Objects.requireNonNull(key, "key must not be null");
            guard(() -> limits.checkKey(key, "key"));
            return read(consistency, timeout, () -> stateMachine.get(key));
        }

        @Override
        public Scan scan(KeyRange range, int limit, ReadConsistency consistency, Duration timeout) {
            Objects.requireNonNull(range, "range must not be null");
            if (limit < 0) {
                throw new NodeException.InvalidRequest("limit must not be negative", null);
            }
            return read(consistency, timeout, () -> stateMachine.scan(range, limit));
        }

        @Override
        public TxnResult transact(Txn txn, Duration timeout) {
            Objects.requireNonNull(txn, "txn must not be null");
            guard(() -> limits.check(txn));
            proposals.incrementAndGet();
            try {
                return await(raft.propose(TxnCodec.encode(txn)), timeout, Kind.WRITE);
            } catch (NodeException e) {
                failedProposals.incrementAndGet();
                throw e;
            }
        }

        private static void guard(Runnable check) {
            try {
                check.run();
            } catch (IllegalArgumentException e) {
                throw new NodeException.InvalidRequest(e.getMessage(), e);
            }
        }
    }

    private final class Cluster implements ClusterView {
        @Override
        public RaftStatus status() {
            return raft.status();
        }

        @Override
        public Membership membership() {
            return raft.membership();
        }

        @Override
        public void transferLeadership(NodeId target, Duration timeout) {
            await(raft.transferLeadership(target), timeout, Kind.READ);
        }
    }
}
