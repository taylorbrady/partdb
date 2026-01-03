package io.partdb.server;

import io.partdb.raft.Membership;
import io.partdb.raft.RaftStorage;
import io.partdb.raft.RaftTransport;
import io.partdb.server.grpc.KvServer;
import io.partdb.server.raft.DurableRaftStorage;
import io.partdb.server.raft.GrpcRaftTransport;
import io.partdb.server.raft.GrpcRaftTransportConfig;
import io.partdb.server.raft.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final KvStore kvStore;
    private final RaftTransport raftTransport;
    private final RaftStorage raftStorage;
    private final RaftNode raftNode;
    private final Proposer proposer;
    private final Lessor lessor;
    private final KvServer kvServer;

    public PartDbServer(PartDbServerConfig config) {
        this(config, null, null);
    }

    public PartDbServer(PartDbServerConfig config, RaftTransport transport, RaftStorage storage) {
        this.config = config;
        this.kvStore = KvStore.open(
            config.dataDirectory().resolve("db"),
            config.storeConfig()
        );

        var membership = createMembership(config);
        this.raftTransport = transport != null ? transport : createDefaultTransport();
        this.raftStorage = storage != null ? storage : createDefaultStorage(config.dataDirectory(), membership);

        this.raftNode = RaftNode.builder()
            .nodeId(config.nodeId())
            .membership(membership)
            .config(config.raftConfig())
            .transport(raftTransport)
            .storage(raftStorage)
            .stateMachine(kvStore)
            .tickInterval(config.tickInterval())
            .build();

        this.proposer = new Proposer(raftNode);
        this.lessor = new Lessor(raftNode, proposer, kvStore.leases());
        this.kvServer = new KvServer(proposer, lessor, kvStore, config.kvServerConfig());
    }

    private static Membership createMembership(PartDbServerConfig config) {
        if (config.peerAddresses().isEmpty()) {
            return Membership.ofVoters(config.nodeId());
        }
        return Membership.ofVoters(config.peerIds().toArray(String[]::new));
    }

    private RaftTransport createDefaultTransport() {
        var transportConfig = GrpcRaftTransportConfig.create(
            config.nodeId(),
            config.raftPort(),
            config.peerAddresses()
        );
        return new GrpcRaftTransport(transportConfig);
    }

    private static RaftStorage createDefaultStorage(Path dataDirectory, Membership membership) {
        Path raftDir = dataDirectory.resolve("raft");
        if (Files.exists(raftDir.resolve("wal"))) {
            return DurableRaftStorage.open(raftDir);
        } else {
            return DurableRaftStorage.create(raftDir, membership);
        }
    }

    public void start() throws IOException {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Starting PartDB server");
        kvServer.start();
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .addKeyValue("kvPort", config.kvServerConfig().port())
            .addKeyValue("raftPort", config.raftPort())
            .log("PartDB server started");
    }

    @Override
    public void close() {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Shutting down PartDB server");
        lessor.close();
        kvServer.close();
        raftNode.close();
        raftTransport.close();
        kvStore.close();
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("PartDB server shut down");
    }
}
