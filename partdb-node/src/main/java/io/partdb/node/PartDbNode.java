package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;
import io.partdb.consensus.ConsensusRole;
import io.partdb.consensus.ConsensusRuntimeFactory;
import io.partdb.node.admin.NodeAdmin;
import io.partdb.node.admin.NodeMetrics;
import io.partdb.node.admin.PartDbBackup;
import io.partdb.node.admin.StorageMetrics;
import io.partdb.node.cluster.ClusterView;
import io.partdb.node.cluster.NodeRole;
import io.partdb.node.cluster.NodeStatus;
import io.partdb.node.command.KvCommand;
import io.partdb.node.kv.KeyRange;
import io.partdb.node.kv.KeyValueEntry;
import io.partdb.node.kv.KeyValueStore;
import io.partdb.node.kv.ReadConsistency;
import io.partdb.node.kv.ScanCursor;
import io.partdb.node.kv.Transaction;
import io.partdb.node.kv.TransactionResult;
import io.partdb.node.kv.VersionedValue;
import io.partdb.node.kv.WriteBatch;
import io.partdb.node.kv.WriteResult;
import io.partdb.node.runtime.NodeFailureMapper;
import io.partdb.node.runtime.NodeRuntime;
import io.partdb.node.state.StoredValue;
import io.partdb.storage.StorageStats;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class PartDbNode implements AutoCloseable {

    private final NodeRuntime runtime;
    private final AtomicLong proposalCount = new AtomicLong();
    private final AtomicLong proposalFailureCount = new AtomicLong();

    private final KeyValueStore keyValues = new KeyValuesView();
    private final ClusterView cluster = new ClusterViewImpl();
    private final NodeAdmin admin = new AdminView();

    private PartDbNode(PartDbNodeConfig config, ConsensusRuntimeFactory runtimeFactory) {
        this.runtime = new NodeRuntime(config, runtimeFactory);
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

    public KeyValueStore keyValues() {
        return keyValues;
    }

    public ClusterView cluster() {
        return cluster;
    }

    public NodeAdmin admin() {
        return admin;
    }

    @Override
    public void close() {
        runtime.close();
    }

    private final class KeyValuesView implements KeyValueStore {
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
                case LINEARIZABLE -> mapFailure(runtime.consensus().readBarrier()
                    .thenApply(_ -> getLocal(key)));
            };
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
                case LINEARIZABLE -> mapFailure(runtime.consensus().readBarrier()
                    .thenApply(_ -> scanLocal(range)));
            };
        }

        @Override
        public CompletionStage<WriteResult> put(Bytes key, Bytes value) {
            return propose(new KvCommand.Put(key, value));
        }

        @Override
        public CompletionStage<WriteResult> delete(Bytes key) {
            Objects.requireNonNull(key, "key must not be null");
            return propose(new KvCommand.Delete(key));
        }

        @Override
        public CompletionStage<WriteResult> write(WriteBatch batch) {
            Objects.requireNonNull(batch, "batch must not be null");
            return propose(new KvCommand.BatchWrite(batch));
        }

        @Override
        public CompletionStage<TransactionResult> transact(Transaction transaction) {
            Objects.requireNonNull(transaction, "transaction must not be null");
            return mapFailure(trackProposal(runtime.commandProposer()
                .proposeTransaction(new KvCommand.CompareAndWrite(transaction))));
        }

        private Optional<VersionedValue> getLocal(Bytes key) {
            return runtime.stateMachine().getValue(key).map(PartDbNode.this::toVersionedValue);
        }

        private ScanCursor<KeyValueEntry> scanLocal(KeyRange range) {
            Stream<KeyValueEntry> stream = runtime.stateMachine().scan(toStorageRange(range))
                .map(entry -> new KeyValueEntry(
                    entry.key(),
                    entry.value(),
                    entry.version()
                ));
            return new StreamBackedScanCursor<>(stream);
        }

        private CompletionStage<WriteResult> propose(KvCommand command) {
            return mapFailure(trackProposal(runtime.commandProposer().propose(command)));
        }
    }

    private final class ClusterViewImpl implements ClusterView {
        @Override
        public NodeStatus status() {
            var status = runtime.consensus().status();
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
            return runtime.consensus().membership();
        }
    }

    private final class AdminView implements NodeAdmin {
        @Override
        public CompletionStage<PartDbBackup> createBackup() {
            return mapFailure(runtime.consensus().readBarrier()
                .thenApply(_ -> new PartDbBackup(
                    runtime.stateMachine().snapshot().data(),
                    runtime.stateMachine().lastAppliedIndex()
                )));
        }

        @Override
        public NodeMetrics nodeMetrics() {
            return new NodeMetrics(proposalCount.get(), proposalFailureCount.get());
        }

        @Override
        public StorageMetrics storageMetrics() {
            StorageStats stats = runtime.stateMachine().storageStats();
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

    private VersionedValue toVersionedValue(StoredValue value) {
        return new VersionedValue(
            value.value(),
            value.revision()
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
        return NodeFailureMapper.map(error);
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
