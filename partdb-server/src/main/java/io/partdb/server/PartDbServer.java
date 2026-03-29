package io.partdb.server;

import io.partdb.raft.RaftStorage;
import io.partdb.raft.RaftTransport;
import io.partdb.node.PartDbNode;
import io.partdb.server.grpc.KvServer;
import io.partdb.server.raft.GrpcRaftTransport;
import io.partdb.server.raft.GrpcRaftTransportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final PartDbNode node;
    private final KvServer kvServer;

    public PartDbServer(PartDbServerConfig config) {
        this(config, null, null);
    }

    public PartDbServer(PartDbServerConfig config, RaftTransport transport, RaftStorage storage) {
        this.config = config;
        RaftTransport raftTransport = transport != null ? transport : createDefaultTransport();
        this.node = new PartDbNode(config.nodeConfig(), raftTransport, storage);

        String selfAddress = config.nodeId() + ":" + config.kvServerConfig().port();
        this.kvServer = new KvServer(
            node.proposer(),
            node.lessor(),
            node.kvStore(),
            node.raftNode(),
            config.peerAddresses(),
            selfAddress,
            config.kvServerConfig()
        );
    }

    private RaftTransport createDefaultTransport() {
        var transportConfig = GrpcRaftTransportConfig.create(
            config.nodeId(),
            config.raftPort(),
            config.peerAddresses()
        );
        return new GrpcRaftTransport(transportConfig);
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
        kvServer.close();
        node.close();
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("PartDB server shut down");
    }
}
