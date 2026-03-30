package io.partdb.node;

import io.partdb.node.command.CommandProposer;
import io.partdb.node.kv.KvStore;
import io.partdb.node.lease.LeaseManager;
import io.partdb.node.transport.ConsensusTransport;
import io.partdb.node.raft.DurableRaftStorage;
import io.partdb.node.raft.RaftNode;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public final class PartDbNode implements AutoCloseable {

    private final KvStore kvStore;
    private final RaftNode raftNode;
    private final CommandProposer proposer;
    private final LeaseManager leaseManager;

    public PartDbNode(PartDbNodeConfig config, ConsensusTransport transport) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");

        this.kvStore = KvStore.open(
            config.dataDirectory().resolve("db"),
            config.storageConfig()
        );

        var membership = config.raftMembership();
        var raftStorage = createDefaultStorage(config.dataDirectory(), membership);

        this.raftNode = RaftNode.builder()
            .nodeId(config.nodeId())
            .membership(membership)
            .config(config.raftConfig())
            .transport(new ConsensusTransportAdapter(transport))
            .storage(raftStorage)
            .stateMachine(kvStore)
            .tickInterval(config.tickInterval())
            .build();

        this.proposer = new CommandProposer(raftNode);
        this.leaseManager = new LeaseManager(raftNode, proposer, kvStore.leaseRegistry());
    }

    public Optional<byte[]> get(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        return kvStore.get(key.clone())
            .map(byte[]::clone);
    }

    public Stream<KeyValueEntry> scan(byte[] startKey, byte[] endKey) {
        byte[] scanStart = startKey != null ? startKey.clone() : null;
        byte[] scanEnd = endKey != null ? endKey.clone() : null;
        return kvStore.scan(scanStart, scanEnd)
            .map(entry -> new KeyValueEntry(
                entry.key(),
                entry.value(),
                entry.version(),
                entry.leaseId()
            ));
    }

    public CompletableFuture<Long> put(byte[] key, byte[] value, long leaseId) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        return proposer.put(key.clone(), value.clone(), leaseId);
    }

    public CompletableFuture<Long> delete(byte[] key) {
        Objects.requireNonNull(key, "key must not be null");
        return proposer.delete(key.clone());
    }

    public CompletableFuture<Long> grantLease(long ttlNanos) {
        if (ttlNanos <= 0) {
            throw new IllegalArgumentException("ttlNanos must be positive");
        }
        return leaseManager.grant(ttlNanos);
    }

    public CompletableFuture<Long> revokeLease(long leaseId) {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        return leaseManager.revoke(leaseId);
    }

    public CompletableFuture<Long> keepAliveLease(long leaseId) {
        if (leaseId <= 0) {
            throw new IllegalArgumentException("leaseId must be positive");
        }
        return leaseManager.keepAlive(leaseId);
    }

    public NodeMembership membership() {
        return NodeMembership.fromRaftMembership(raftNode.membership());
    }

    public NodeStatus status() {
        return new NodeStatus(
            raftNode.nodeId(),
            NodeRole.fromRaftRole(raftNode.role()),
            raftNode.currentTerm(),
            raftNode.leaderId(),
            raftNode.commitIndex(),
            raftNode.lastAppliedIndex(),
            raftNode.isRunning()
        );
    }

    public String nodeId() {
        return raftNode.nodeId();
    }

    public Optional<String> leaderId() {
        return raftNode.leaderId();
    }

    @Override
    public void close() {
        leaseManager.close();
        raftNode.close();
        kvStore.close();
    }

    private static io.partdb.raft.RaftStorage createDefaultStorage(Path dataDirectory, io.partdb.raft.Membership membership) {
        Path raftDir = dataDirectory.resolve("raft");
        if (Files.exists(raftDir.resolve("wal"))) {
            return DurableRaftStorage.open(raftDir);
        }
        return DurableRaftStorage.create(raftDir, membership);
    }
}
