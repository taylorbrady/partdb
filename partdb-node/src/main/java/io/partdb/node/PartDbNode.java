package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;
import io.partdb.consensus.ConsensusException;
import io.partdb.consensus.ConsensusRole;
import io.partdb.consensus.ConsensusRuntime;
import io.partdb.consensus.ConsensusRuntimeFactory;
import io.partdb.node.cluster.ClusterView;
import io.partdb.node.cluster.NodeRole;
import io.partdb.node.cluster.NodeStatus;
import io.partdb.node.internal.command.PartDbCommands;
import io.partdb.node.internal.command.PartDbCommandExecutor;
import io.partdb.node.kv.DeleteResult;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValueEntry;
import io.partdb.node.kv.KeyValueOperations;
import io.partdb.node.kv.PutRequest;
import io.partdb.node.kv.PutResult;
import io.partdb.node.kv.ReadConsistency;
import io.partdb.node.kv.ScanCursor;
import io.partdb.node.kv.VersionedValue;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteBatchResult;
import io.partdb.node.metrics.MaintenanceOperations;
import io.partdb.node.metrics.NodeMetrics;
import io.partdb.node.metrics.StorageMetrics;
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
    private final ConsensusRuntime consensus;
    private final PartDbCommandExecutor commandExecutor;
    private final AtomicLong proposalCount = new AtomicLong();
    private final AtomicLong proposalFailureCount = new AtomicLong();

    private final KeyValueOperations keyValues = new KeyValuesView();
    private final ClusterView cluster = new ClusterViewImpl();
    private final MaintenanceOperations maintenance = new MaintenanceView();

    private PartDbNode(PartDbNodeConfig config, ConsensusRuntimeFactory runtimeFactory) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(runtimeFactory, "runtimeFactory must not be null");

        this.stateMachine = PartDbStateMachine.open(
            config.dataDirectory().resolve("db"),
            config.storage().toStorageOptions()
        );

        try {
            this.consensus = runtimeFactory.open(
                config.dataDirectory().resolve("consensus"),
                config.toConsensusConfig(),
                stateMachine
            );
            this.commandExecutor = new PartDbCommandExecutor(consensus);
        } catch (RuntimeException | Error e) {
            stateMachine.close();
            throw e;
        }
    }

    public static PartDbNode open(PartDbNodeConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        if (config.memberIds().size() != 1) {
            throw new IllegalArgumentException("open(config) only supports single-node membership");
        }
        return new PartDbNode(config, ConsensusRuntimeFactory.singleNode());
    }

    public static PartDbNode open(PartDbNodeConfig config, ConsensusRuntimeFactory runtimeFactory) {
        return new PartDbNode(config, runtimeFactory);
    }

    public KeyValueOperations keyValues() {
        return keyValues;
    }

    public ClusterView cluster() {
        return cluster;
    }

    public MaintenanceOperations maintenance() {
        return maintenance;
    }

    @Override
    public void close() {
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
                    entry.version()
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
            CompletableFuture<PutResult> proposal = commandExecutor.execute(
                PartDbCommands.put(request.key(), request.value())
            );
            return mapFailure(trackProposal(proposal));
        }

        @Override
        public CompletionStage<DeleteResult> delete(Bytes key) {
            Objects.requireNonNull(key, "key must not be null");
            return mapFailure(trackProposal(commandExecutor.execute(PartDbCommands.delete(key))));
        }

        @Override
        public CompletionStage<WriteBatchResult> writeBatch(WriteBatch batch) {
            Objects.requireNonNull(batch, "batch must not be null");
            return mapFailure(trackProposal(commandExecutor.execute(PartDbCommands.writeBatch(batch))));
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
            return consensus.membership();
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
            value.modRevision()
        );
    }

    private static NodeRole toNodeRole(ConsensusRole role) {
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

}
