package io.partdb.node;

import io.partdb.bytes.Bytes;
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
import io.partdb.node.state.KvCommand;
import io.partdb.node.state.KvCommandCodec;
import io.partdb.node.state.KvCommandResult;
import io.partdb.node.state.KvCommandResultCodec;
import io.partdb.node.state.PartDbStateMachine;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.runtime.RaftRuntime;
import io.partdb.raft.runtime.RaftStatus;
import io.partdb.raft.runtime.RaftTransport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Stream;

public final class PartDbNode implements AutoCloseable {

    private static final Executor BACKGROUND = task -> Thread.ofVirtual().name("partdb-background").start(task);

    private final PartDbStateMachine stateMachine;
    private final RaftRuntime raft;
    private final AtomicLong proposalCount = new AtomicLong();
    private final AtomicLong proposalFailureCount = new AtomicLong();

    private final KeyValueStore keyValues = new KeyValues();
    private final ClusterView cluster = new Cluster();
    private final NodeAdmin admin = new Admin();

    private PartDbNode(PartDbStateMachine stateMachine, RaftRuntime raft) {
        this.stateMachine = stateMachine;
        this.raft = raft;
    }

    public static PartDbNode open(PartDbNodeConfig config) {
        Objects.requireNonNull(config, "config must not be null");
        if (config.raft().membership().memberIds().size() != 1) {
            throw new IllegalArgumentException("A node without a transport must be the only cluster member");
        }
        return open(config, RaftTransport.standalone());
    }

    public static PartDbNode open(PartDbNodeConfig config, RaftTransport transport) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");

        PartDbStateMachine stateMachine;
        try {
            stateMachine = PartDbStateMachine.open(storageDirectory(config));
        } catch (RuntimeException e) {
            transport.close();
            throw e;
        }
        try {
            return new PartDbNode(stateMachine, RaftRuntime.open(raftDirectory(config), config.raft(), transport, stateMachine));
        } catch (RuntimeException e) {
            stateMachine.close();
            throw e;
        }
    }

    public static long restore(PartDbNodeConfig config, Path backup) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(backup, "backup must not be null");

        Path dataDirectory = config.dataDirectory();
        try {
            if (Files.exists(dataDirectory)) {
                try (Stream<Path> entries = Files.list(dataDirectory)) {
                    if (entries.findAny().isPresent()) {
                        throw new IllegalArgumentException("dataDirectory must be empty for restore: " + dataDirectory);
                    }
                }
            }
            Files.createDirectories(dataDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to prepare data directory: " + dataDirectory, e);
        }

        try {
            long revision;
            try (
                var stateMachine = PartDbStateMachine.open(storageDirectory(config));
                InputStream in = new BufferedInputStream(Files.newInputStream(backup))
            ) {
                revision = stateMachine.load(in);
            }
            RaftRuntime.bootstrap(raftDirectory(config), config.raft(), revision, backup);
            return revision;
        } catch (IOException e) {
            clear(dataDirectory, e);
            throw new UncheckedIOException("Failed to read backup: " + backup, e);
        } catch (RuntimeException e) {
            clear(dataDirectory, e);
            throw e;
        }
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
        try {
            raft.close();
        } finally {
            stateMachine.close();
        }
    }

    private CompletableFuture<KvCommandResult> propose(KvCommand command) {
        proposalCount.incrementAndGet();
        return raft.propose(KvCommandCodec.encode(command))
            .thenApply(KvCommandResultCodec::decode)
            .whenComplete((_, error) -> {
                if (error != null) {
                    proposalFailureCount.incrementAndGet();
                }
            });
    }

    private CompletableFuture<WriteResult> submit(KvCommand command) {
        return mapFailure(propose(command).thenApply(result -> switch (result) {
            case KvCommandResult.Applied(long revision) -> new WriteResult(revision);
            case KvCommandResult.ConditionFailed _ -> throw new IllegalStateException("Unconditional write reported a failed condition");
        }));
    }

    private <T> CompletableFuture<T> afterReadBarrier(ReadConsistency consistency, Supplier<T> read) {
        return switch (consistency) {
            case LOCAL -> CompletableFuture.completedFuture(read.get());
            case LINEARIZABLE -> mapFailure(raft.readBarrier().thenApply(_ -> read.get()));
        };
    }

    private long writeBackup(Path target) {
        Path staging = target.resolveSibling(target.getFileName() + ".tmp");
        try {
            long revision;
            try (FileChannel channel = FileChannel.open(
                staging,
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE
            )) {
                OutputStream out = new BufferedOutputStream(Channels.newOutputStream(channel));
                revision = stateMachine.checkpoint(out);
                out.flush();
                channel.force(true);
            }
            Files.move(staging, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            return revision;
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write backup: " + target, e);
        }
    }

    private static <T> CompletableFuture<T> mapFailure(CompletableFuture<T> future) {
        return future.exceptionallyCompose(error -> CompletableFuture.failedFuture(FailureMapper.map(error)));
    }

    private static Path storageDirectory(PartDbNodeConfig config) {
        return config.dataDirectory().resolve("db");
    }

    private static Path raftDirectory(PartDbNodeConfig config) {
        return config.dataDirectory().resolve("raft");
    }

    private static void clear(Path directory, Exception failure) {
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).filter(path -> !path.equals(directory)).toList()) {
                Files.deleteIfExists(path);
            }
        } catch (IOException e) {
            failure.addSuppressed(e);
        }
    }

    private final class KeyValues implements KeyValueStore {
        @Override
        public CompletionStage<Optional<VersionedValue>> get(Bytes key) {
            return get(key, ReadConsistency.LINEARIZABLE);
        }

        @Override
        public CompletionStage<Optional<VersionedValue>> get(Bytes key, ReadConsistency consistency) {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(consistency, "consistency must not be null");
            return afterReadBarrier(consistency, () -> stateMachine.get(key));
        }

        @Override
        public CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range) {
            return scan(range, ReadConsistency.LINEARIZABLE);
        }

        @Override
        public CompletionStage<ScanCursor<KeyValueEntry>> scan(KeyRange range, ReadConsistency consistency) {
            Objects.requireNonNull(range, "range must not be null");
            Objects.requireNonNull(consistency, "consistency must not be null");
            return afterReadBarrier(consistency, () -> stateMachine.scan(range));
        }

        @Override
        public CompletionStage<WriteResult> put(Bytes key, Bytes value) {
            return submit(new KvCommand.Put(key, value));
        }

        @Override
        public CompletionStage<WriteResult> delete(Bytes key) {
            return submit(new KvCommand.Delete(key));
        }

        @Override
        public CompletionStage<WriteResult> write(WriteBatch batch) {
            return submit(new KvCommand.BatchWrite(batch));
        }

        @Override
        public CompletionStage<TransactionResult> transact(Transaction transaction) {
            return mapFailure(propose(new KvCommand.CompareAndWrite(transaction)).thenApply(result -> switch (result) {
                case KvCommandResult.Applied(long revision) -> new TransactionResult.Applied(revision);
                case KvCommandResult.ConditionFailed _ -> new TransactionResult.ConditionFailed();
            }));
        }
    }

    private final class Cluster implements ClusterView {
        @Override
        public RaftStatus status() {
            return raft.status();
        }

        @Override
        public RaftMembership membership() {
            return raft.membership();
        }
    }

    private final class Admin implements NodeAdmin {
        @Override
        public CompletionStage<Long> backup(Path target) {
            Objects.requireNonNull(target, "target must not be null");
            return mapFailure(raft.readBarrier().thenApplyAsync(_ -> writeBackup(target), BACKGROUND));
        }

        @Override
        public NodeMetrics nodeMetrics() {
            return new NodeMetrics(proposalCount.get(), proposalFailureCount.get());
        }

        @Override
        public StorageMetrics storageMetrics() {
            var stats = stateMachine.storageStats();
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
}
