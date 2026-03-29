package io.partdb.node;

import io.partdb.node.command.CommandProposer;
import io.partdb.node.kv.KvStore;
import io.partdb.node.lease.LeaseManager;
import io.partdb.node.raft.DurableRaftStorage;
import io.partdb.node.raft.RaftNode;
import io.partdb.raft.Membership;
import io.partdb.raft.RaftStorage;
import io.partdb.raft.RaftTransport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public final class PartDbNode implements AutoCloseable {

    private final KvStore kvStore;
    private final RaftNode raftNode;
    private final CommandProposer proposer;
    private final LeaseManager leaseManager;

    public PartDbNode(PartDbNodeConfig config, RaftTransport transport) {
        this(config, transport, null);
    }

    public PartDbNode(PartDbNodeConfig config, RaftTransport transport, RaftStorage storage) {
        Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");

        this.kvStore = KvStore.open(
            config.dataDirectory().resolve("db"),
            config.storeConfig()
        );

        var membership = createMembership(config);
        RaftStorage raftStorage = storage != null ? storage : createDefaultStorage(config.dataDirectory(), membership);

        this.raftNode = RaftNode.builder()
            .nodeId(config.nodeId())
            .membership(membership)
            .config(config.raftConfig())
            .transport(transport)
            .storage(raftStorage)
            .stateMachine(kvStore)
            .tickInterval(config.tickInterval())
            .build();

        this.proposer = new CommandProposer(raftNode);
        this.leaseManager = new LeaseManager(raftNode, proposer, kvStore.leaseRegistry());
    }

    public Optional<byte[]> get(byte[] key) {
        return kvStore.get(key);
    }

    public Stream<KeyValueEntry> scan(byte[] startKey, byte[] endKey) {
        return kvStore.scan(startKey, endKey)
            .map(entry -> new KeyValueEntry(
                entry.key(),
                entry.value(),
                entry.version(),
                entry.leaseId()
            ));
    }

    public CompletableFuture<Long> put(byte[] key, byte[] value, long leaseId) {
        return proposer.put(key, value, leaseId);
    }

    public CompletableFuture<Long> delete(byte[] key) {
        return proposer.delete(key);
    }

    public CompletableFuture<Long> grantLease(long ttlNanos) {
        return leaseManager.grant(ttlNanos);
    }

    public CompletableFuture<Long> revokeLease(long leaseId) {
        return leaseManager.revoke(leaseId);
    }

    public CompletableFuture<Long> keepAliveLease(long leaseId) {
        return leaseManager.keepAlive(leaseId);
    }

    public Membership membership() {
        return raftNode.membership();
    }

    public NodeStatus status() {
        return new NodeStatus(
            raftNode.nodeId(),
            raftNode.role(),
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

    private static Membership createMembership(PartDbNodeConfig config) {
        if (config.peerAddresses().isEmpty()) {
            return Membership.ofVoters(config.nodeId());
        }
        return Membership.ofVoters(config.peerIds().toArray(String[]::new));
    }

    private static RaftStorage createDefaultStorage(Path dataDirectory, Membership membership) {
        Path raftDir = dataDirectory.resolve("raft");
        if (Files.exists(raftDir.resolve("wal"))) {
            return DurableRaftStorage.open(raftDir);
        }
        return DurableRaftStorage.create(raftDir, membership);
    }
}
