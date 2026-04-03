package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.ConsensusException;
import io.partdb.consensus.ConsensusNode;
import io.partdb.node.cluster.ClusterMembership;
import io.partdb.node.cluster.ClusterView;
import io.partdb.node.cluster.NodeRole;
import io.partdb.node.cluster.NodeStatus;
import io.partdb.node.internal.command.CommandProposer;
import io.partdb.node.internal.command.PartDbCommandResult;
import io.partdb.node.internal.replication.ConsensusTransportAdapter;
import io.partdb.node.kv.DeleteResult;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValueEntry;
import io.partdb.node.kv.KeyValueOperations;
import io.partdb.node.kv.PutRequest;
import io.partdb.node.kv.PutResult;
import io.partdb.node.kv.ReadConsistency;
import io.partdb.node.kv.ScanCursor;
import io.partdb.node.kv.VersionedValue;
import io.partdb.node.lease.LeaseGrant;
import io.partdb.node.lease.LeaseId;
import io.partdb.node.lease.LeaseKeepAliveResult;
import io.partdb.node.lease.LeaseOperations;
import io.partdb.node.lease.LeaseRevokeResult;
import io.partdb.node.lease.LeaseService;
import io.partdb.node.metrics.MaintenanceOperations;
import io.partdb.node.metrics.NodeMetrics;
import io.partdb.node.metrics.StorageMetrics;
import io.partdb.node.replication.ReplicationRpc;
import io.partdb.node.replication.ReplicationTransport;
import io.partdb.node.recovery.LogicalBackup;
import io.partdb.node.state.PartDbStateMachine;
import io.partdb.storage.StorageStats;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class PartDbNode implements AutoCloseable {

    private final PartDbStateMachine stateMachine;
    private final ConsensusNode consensus;
    private final CommandProposer proposer;
    private final LeaseService leaseService;
    private final AtomicLong proposalCount = new AtomicLong();
    private final AtomicLong proposalFailureCount = new AtomicLong();

    private final KeyValueOperations keyValues = new KeyValuesView();
    private final LeaseOperations leases = new LeaseView();
    private final ClusterView cluster = new ClusterViewImpl();
    private final MaintenanceOperations maintenance = new MaintenanceView();

    private PartDbNode(PartDbNodeConfig config, ReplicationTransport transport) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");

        this.stateMachine = PartDbStateMachine.open(
            config.dataDirectory().resolve("db"),
            config.storage().toStorageOptions()
        );

        this.consensus = ConsensusNode.open(
            config.dataDirectory().resolve("consensus"),
            config.toConsensusConfig(),
            new ConsensusTransportAdapter(transport),
            stateMachine
        );

        this.proposer = new CommandProposer(consensus);
        this.leaseService = new LeaseService(consensus, proposer, stateMachine.leaseRegistry());
    }

    public static PartDbNode open(PartDbNodeConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        if (config.memberIds().size() != 1) {
            throw new IllegalArgumentException("open(config) only supports single-node membership");
        }
        return new PartDbNode(config, new SingleNodeTransport());
    }

    public static PartDbNode open(PartDbNodeConfig config, ReplicationTransport transport) {
        return new PartDbNode(config, transport);
    }

    public KeyValueOperations keyValues() {
        return keyValues;
    }

    public LeaseOperations leases() {
        return leases;
    }

    public ClusterView cluster() {
        return cluster;
    }

    public MaintenanceOperations maintenance() {
        return maintenance;
    }

    @Override
    public void close() {
        leaseService.close();
        consensus.close();
        stateMachine.close();
    }

    private final class KeyValuesView implements KeyValueOperations {
        @Override
        public Optional<VersionedValue> getLocal(Bytes key) {
            Objects.requireNonNull(key, "key must not be null");
            return stateMachine.getLocalValue(key).map(PartDbNode.this::toVersionedValue);
        }

        @Override
        public CompletionStage<Optional<VersionedValue>> get(Bytes key) {
            return get(key, ReadConsistency.LINEARIZABLE);
        }

        @Override
        public CompletionStage<Optional<VersionedValue>> get(Bytes key, ReadConsistency consistency) {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(consistency, "consistency must not be null");
            return switch (consistency) {
                case LOCAL -> CompletableFuture.completedFuture(getLocal(key));
                case LINEARIZABLE -> mapFailure(consensus.linearizableBarrier()
                    .thenApply(_ -> getLocal(key)));
            };
        }

        @Override
        public ScanCursor<KeyValueEntry> scanLocal(KeyRange range) {
            Objects.requireNonNull(range, "range must not be null");
            Stream<KeyValueEntry> stream = stateMachine.scanLocal(toStorageRange(range))
                .map(entry -> new KeyValueEntry(
                    entry.key(),
                    entry.value(),
                    entry.version(),
                    entry.leaseId() == 0 ? Optional.empty() : Optional.of(LeaseId.of(entry.leaseId()))
                ));
            return new StreamBackedScanCursor<>(stream);
        }

        @Override
        public CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range) {
            return scan(range, ReadConsistency.LINEARIZABLE);
        }

        @Override
        public CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range, ReadConsistency consistency) {
            Objects.requireNonNull(range, "range must not be null");
            Objects.requireNonNull(consistency, "consistency must not be null");
            return switch (consistency) {
                case LOCAL -> CompletableFuture.completedFuture(scanLocal(range));
                case LINEARIZABLE -> mapFailure(consensus.linearizableBarrier()
                    .thenApply(_ -> scanLocal(range)));
            };
        }

        @Override
        public CompletionStage<PutResult> put(PutRequest request) {
            Objects.requireNonNull(request, "request must not be null");
            CompletableFuture<PartDbCommandResult> proposal = request.leaseId().isPresent()
                ? proposer.put(request.key(), request.value(), request.leaseId().orElseThrow().value())
                : proposer.put(request.key(), request.value(), 0);
            return mapFailure(trackProposal(proposal).thenApply(PartDbNode::toPutResult));
        }

        @Override
        public CompletionStage<DeleteResult> delete(Bytes key) {
            Objects.requireNonNull(key, "key must not be null");
            return mapFailure(trackProposal(proposer.delete(key)).thenApply(PartDbNode::toDeleteResult));
        }
    }

    private final class LeaseView implements LeaseOperations {
        @Override
        public CompletionStage<LeaseGrant> grant(Duration ttl) {
            Objects.requireNonNull(ttl, "ttl must not be null");
            if (ttl.isZero() || ttl.isNegative()) {
                throw new IllegalArgumentException("ttl must be positive");
            }
            return mapFailure(trackProposal(leaseService.grant(ttl))
                .thenApply(PartDbNode::toLeaseGrant));
        }

        @Override
        public CompletionStage<LeaseKeepAliveResult> keepAlive(LeaseId leaseId) {
            Objects.requireNonNull(leaseId, "leaseId must not be null");
            return mapFailure(trackProposal(leaseService.keepAlive(leaseId))
                .thenApply(PartDbNode::toLeaseKeepAliveResult));
        }

        @Override
        public CompletionStage<LeaseRevokeResult> revoke(LeaseId leaseId) {
            Objects.requireNonNull(leaseId, "leaseId must not be null");
            return mapFailure(trackProposal(leaseService.revoke(leaseId))
                .thenApply(PartDbNode::toLeaseRevokeResult));
        }
    }

    private final class ClusterViewImpl implements ClusterView {
        @Override
        public NodeStatus status() {
            var status = consensus.status();
            Optional<Instant> lastLeaderChangeTime = status.lastLeaderChangeEpochMillis() > 0
                ? Optional.of(Instant.ofEpochMilli(status.lastLeaderChangeEpochMillis()))
                : Optional.empty();
            return new NodeStatus(
                status.nodeId(),
                toNodeRole(status.role()),
                status.term(),
                status.leaderId(),
                status.commitIndex(),
                status.appliedIndex(),
                lastLeaderChangeTime,
                status.running()
            );
        }

        @Override
        public ClusterMembership membership() {
            var membership = consensus.membership();
            return new ClusterMembership(membership.voters(), membership.learners());
        }
    }

    private final class MaintenanceView implements MaintenanceOperations {
        @Override
        public CompletionStage<LogicalBackup> createBackup() {
            return mapFailure(consensus.linearizableBarrier()
                .thenApply(_ -> new LogicalBackup(stateMachine.snapshot(), stateMachine.lastAppliedIndex())));
        }

        @Override
        public NodeMetrics nodeMetrics() {
            return new NodeMetrics(proposalCount.get(), proposalFailureCount.get());
        }

        @Override
        public StorageMetrics storageMetrics() {
            StorageStats stats = stateMachine.storageStats();
            return new StorageMetrics(
                stats.activeMemtableBytes(),
                stats.immutableMemtableCount(),
                stats.sstableCount(),
                stats.totalSstableBytes(),
                stats.activeCompactions(),
                stats.completedCompactions(),
                stats.failedCompactions(),
                Duration.ofMillis(stats.lastCompactionDurationMillis()),
                stats.checkpointCount(),
                stats.restoreCount(),
                Duration.ofMillis(stats.lastCheckpointDurationMillis()),
                Duration.ofMillis(stats.lastRestoreDurationMillis())
            );
        }
    }

    private <T> CompletableFuture<T> trackProposal(CompletableFuture<T> future) {
        proposalCount.incrementAndGet();
        return future.whenComplete((ignored, error) -> {
            if (error != null) {
                proposalFailureCount.incrementAndGet();
            }
        });
    }

    private <T> CompletableFuture<T> mapFailure(CompletionStage<T> stage) {
        CompletableFuture<T> mapped = new CompletableFuture<>();
        stage.whenComplete((value, error) -> {
            if (error == null) {
                mapped.complete(value);
            } else {
                mapped.completeExceptionally(mapThrowable(error));
            }
        });
        return mapped;
    }

    private VersionedValue toVersionedValue(PartDbStateMachine.LocalValue value) {
        return new VersionedValue(
            value.value(),
            value.modRevision(),
            value.leaseId() == 0 ? Optional.empty() : Optional.of(LeaseId.of(value.leaseId()))
        );
    }

    private static PutResult toPutResult(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.PutApplied(long modRevision) -> new PutResult(modRevision);
            case PartDbCommandResult.LeaseNotFound(long leaseId) -> throw new PartDbException.LeaseNotFound(leaseId);
            default -> throw unexpectedResult("put", result);
        };
    }

    private static DeleteResult toDeleteResult(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.DeleteApplied(long modRevision) -> new DeleteResult(modRevision);
            default -> throw unexpectedResult("delete", result);
        };
    }

    private static LeaseGrant toLeaseGrant(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.LeaseGranted(long modRevision, long leaseId, long ttlNanos) ->
                new LeaseGrant(LeaseId.of(leaseId), Duration.ofNanos(ttlNanos), modRevision);
            default -> throw unexpectedResult("grantLease", result);
        };
    }

    private static LeaseKeepAliveResult toLeaseKeepAliveResult(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.LeaseKeptAlive(long modRevision, long leaseId, long ttlNanos) ->
                new LeaseKeepAliveResult(LeaseId.of(leaseId), Duration.ofNanos(ttlNanos), modRevision);
            case PartDbCommandResult.LeaseNotFound(long leaseId) -> throw new PartDbException.LeaseNotFound(leaseId);
            default -> throw unexpectedResult("keepAliveLease", result);
        };
    }

    private static LeaseRevokeResult toLeaseRevokeResult(PartDbCommandResult result) {
        return switch (result) {
            case PartDbCommandResult.LeaseRevoked(long modRevision, long leaseId, long deletedKeyCount) ->
                new LeaseRevokeResult(LeaseId.of(leaseId), modRevision, deletedKeyCount);
            case PartDbCommandResult.LeaseNotFound(long leaseId) -> throw new PartDbException.LeaseNotFound(leaseId);
            default -> throw unexpectedResult("revokeLease", result);
        };
    }

    private static IllegalStateException unexpectedResult(String operation, PartDbCommandResult result) {
        return new IllegalStateException("Unexpected command result for " + operation + ": " + result);
    }

    private static NodeRole toNodeRole(io.partdb.consensus.ConsensusRole role) {
        return switch (role) {
            case FOLLOWER -> NodeRole.FOLLOWER;
            case PRE_CANDIDATE -> NodeRole.PRE_CANDIDATE;
            case CANDIDATE -> NodeRole.CANDIDATE;
            case LEADER -> NodeRole.LEADER;
        };
    }

    private static Throwable mapThrowable(Throwable error) {
        Throwable cause = unwrap(error);
        return switch (cause) {
            case PartDbException _ -> cause;
            case ConsensusException.NotLeader e -> new PartDbException.NotLeader(e.leaderId().orElse(null));
            case ConsensusException.Shutdown _ -> new PartDbException.NodeClosed();
            case IllegalArgumentException _ -> cause;
            default -> new PartDbException.StorageFailure(
                cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName(),
                cause
            );
        };
    }

    private static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException || error instanceof ExecutionException) {
            return error.getCause() != null ? error.getCause() : error;
        }
        return error;
    }

    private static io.partdb.storage.KeyRange toStorageRange(KeyRange range) {
        return switch ((range.startInclusive().isPresent() ? 1 : 0) | (range.endExclusive().isPresent() ? 2 : 0)) {
            case 0 -> io.partdb.storage.KeyRange.all();
            case 1 -> io.partdb.storage.KeyRange.from(range.startInclusive().orElseThrow());
            case 2 -> io.partdb.storage.KeyRange.until(range.endExclusive().orElseThrow());
            case 3 -> io.partdb.storage.KeyRange.between(
                range.startInclusive().orElseThrow(),
                range.endExclusive().orElseThrow()
            );
            default -> throw new IllegalStateException("Unexpected key range state");
        };
    }

    private static final class StreamBackedScanCursor<T> implements ScanCursor<T> {
        private final Stream<T> stream;
        private final Iterator<T> iterator;
        private boolean closed;

        private StreamBackedScanCursor(Stream<T> stream) {
            this.stream = Objects.requireNonNull(stream, "stream must not be null");
            this.iterator = stream.iterator();
            this.closed = false;
        }

        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            boolean hasNext = iterator.hasNext();
            if (!hasNext) {
                close();
            }
            return hasNext;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                stream.close();
            }
        }
    }

    private static final class SingleNodeTransport implements ReplicationTransport {
        @Override
        public void start(RpcHandler handler) {
        }

        @Override
        public CompletableFuture<ReplicationRpc.Response> send(String to, ReplicationRpc.Request request) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException("single-node transport"));
        }

        @Override
        public void close() {
        }
    }
}
