package io.partdb.node;

import io.partdb.node.raft.DurableRaftStorage;
import io.partdb.node.raft.RaftNode;
import io.partdb.raft.Membership;
import io.partdb.raft.RaftStorage;
import io.partdb.raft.RaftTransport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;

public final class PartDbNode implements AutoCloseable {

    private final PartDbNodeConfig config;
    private final KvStore kvStore;
    private final RaftStorage raftStorage;
    private final RaftNode raftNode;
    private final Proposer proposer;
    private final Lessor lessor;

    public PartDbNode(PartDbNodeConfig config, RaftTransport transport) {
        this(config, transport, null);
    }

    public PartDbNode(PartDbNodeConfig config, RaftTransport transport, RaftStorage storage) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        Objects.requireNonNull(transport, "transport must not be null");

        this.kvStore = KvStore.open(
            config.dataDirectory().resolve("db"),
            config.storeConfig()
        );

        var membership = createMembership(config);
        this.raftStorage = storage != null ? storage : createDefaultStorage(config.dataDirectory(), membership);

        this.raftNode = RaftNode.builder()
            .nodeId(config.nodeId())
            .membership(membership)
            .config(config.raftConfig())
            .transport(transport)
            .storage(raftStorage)
            .stateMachine(kvStore)
            .tickInterval(config.tickInterval())
            .build();

        this.proposer = new Proposer(raftNode);
        this.lessor = new Lessor(raftNode, proposer, kvStore.leases());
    }

    public PartDbNodeConfig config() {
        return config;
    }

    public KvStore kvStore() {
        return kvStore;
    }

    public RaftNode raftNode() {
        return raftNode;
    }

    public Proposer proposer() {
        return proposer;
    }

    public Lessor lessor() {
        return lessor;
    }

    @Override
    public void close() {
        lessor.close();
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
