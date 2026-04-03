package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.consensus.ClusterMembership;
import io.partdb.consensus.ConsensusNode;
import io.partdb.consensus.ConsensusStatus;
import io.partdb.consensus.transport.ConsensusTransport;
import io.partdb.node.command.CommandProposer;
import io.partdb.node.kv.KvStore;
import io.partdb.node.lease.LeaseManager;
import io.partdb.storage.KeyRange;
import io.partdb.storage.StorageStats;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

public final class PartDbNode implements AutoCloseable {

    private final KvStore kvStore;
    private final ConsensusNode consensus;
    private final CommandProposer proposer;
    private final LeaseManager leaseManager;
    private final AtomicLong proposalCount = new AtomicLong();
    private final AtomicLong proposalFailureCount = new AtomicLong();

    public PartDbNode(PartDbNodeConfig config, ConsensusTransport transport) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");

        this.kvStore = KvStore.open(
            config.dataDirectory().resolve("db"),
            config.storageOptions()
        );

        this.consensus = ConsensusNode.open(
            config.dataDirectory().resolve("consensus"),
            config.consensusConfig(),
            transport,
            kvStore
        );

        this.proposer = new CommandProposer(consensus);
        this.leaseManager = new LeaseManager(consensus, proposer, kvStore.leaseRegistry());
    }

    public Optional<Bytes> get(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        return kvStore.get(key);
    }

    public Stream<KeyValueEntry> scan(Optional<Bytes> startKey, Optional<Bytes> endKey) {
        Objects.requireNonNull(startKey, "startKey must not be null");
        Objects.requireNonNull(endKey, "endKey must not be null");
        KeyRange range = switch ((startKey.isPresent() ? 1 : 0) | (endKey.isPresent() ? 2 : 0)) {
            case 0 -> KeyRange.all();
            case 1 -> KeyRange.from(startKey.orElseThrow());
            case 2 -> KeyRange.until(endKey.orElseThrow());
            case 3 -> KeyRange.between(startKey.orElseThrow(), endKey.orElseThrow());
            default -> throw new IllegalStateException("Unexpected key range state");
        };
        return kvStore.scan(range)
            .map(entry -> new KeyValueEntry(
                entry.key(),
                entry.value(),
                entry.version(),
                entry.leaseId()
            ));
    }

    public CompletableFuture<Long> put(Bytes key, Bytes value, long leaseId) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        return trackProposal(proposer.put(key, value, leaseId));
    }

    public CompletableFuture<Long> delete(Bytes key) {
        Objects.requireNonNull(key, "key must not be null");
        return trackProposal(proposer.delete(key));
    }

    public CompletableFuture<Long> grantLease(long ttlNanos) {
        if (ttlNanos <= 0) {
            throw new IllegalArgumentException("ttlNanos must be positive");
        }
        return trackProposal(leaseManager.grant(ttlNanos));
    }

    public CompletableFuture<Long> revokeLease(long leaseId) {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        return trackProposal(leaseManager.revoke(leaseId));
    }

    public CompletableFuture<Long> keepAliveLease(long leaseId) {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        return trackProposal(leaseManager.keepAlive(leaseId));
    }

    public CompletableFuture<Long> linearizableBarrier() {
        return consensus.linearizableBarrier();
    }

    public ClusterMembership membership() {
        return consensus.membership();
    }

    public ConsensusStatus status() {
        return consensus.status();
    }

    public String nodeId() {
        return consensus.status().nodeId();
    }

    public Optional<String> leaderId() {
        return consensus.status().leaderId();
    }

    public long lastLeaderChangeEpochMillis() {
        return consensus.status().lastLeaderChangeEpochMillis();
    }

    public long proposalCount() {
        return proposalCount.get();
    }

    public long proposalFailureCount() {
        return proposalFailureCount.get();
    }

    public StorageStats storageStats() {
        return kvStore.storageStats();
    }

    public long storageActiveMemtableBytes() {
        return storageStats().activeMemtableBytes();
    }

    public int storageImmutableMemtableCount() {
        return storageStats().immutableMemtableCount();
    }

    public int storageSstableCount() {
        return storageStats().sstableCount();
    }

    public long storageTotalSstableBytes() {
        return storageStats().totalSstableBytes();
    }

    public int storageActiveCompactions() {
        return storageStats().activeCompactions();
    }

    public long storageCompletedCompactions() {
        return storageStats().completedCompactions();
    }

    public long storageFailedCompactions() {
        return storageStats().failedCompactions();
    }

    public long storageLastCompactionDurationMillis() {
        return storageStats().lastCompactionDurationMillis();
    }

    public long storageCheckpointCount() {
        return storageStats().checkpointCount();
    }

    public long storageRestoreCount() {
        return storageStats().restoreCount();
    }

    public long storageLastCheckpointDurationMillis() {
        return storageStats().lastCheckpointDurationMillis();
    }

    public long storageLastRestoreDurationMillis() {
        return storageStats().lastRestoreDurationMillis();
    }

    @Override
    public void close() {
        leaseManager.close();
        consensus.close();
        kvStore.close();
    }

    private CompletableFuture<Long> trackProposal(CompletableFuture<Long> future) {
        proposalCount.incrementAndGet();
        return future.whenComplete((ignored, error) -> {
            if (error != null) {
                proposalFailureCount.incrementAndGet();
            }
        });
    }
}
