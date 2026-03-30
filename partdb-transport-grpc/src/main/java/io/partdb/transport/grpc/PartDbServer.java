package io.partdb.transport.grpc;

import io.partdb.raft.RaftStorage;
import io.partdb.raft.RaftTransport;
import io.partdb.node.PartDbNode;
import io.partdb.transport.grpc.raft.GrpcRaftTransport;
import io.partdb.transport.grpc.raft.GrpcRaftTransportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final PartDbNode node;
    private final GrpcServer grpcServer;

    public PartDbServer(PartDbServerConfig config) {
        this(config, null, null);
    }

    PartDbServer(PartDbServerConfig config, RaftTransport transport, RaftStorage storage) {
        this.config = config;
        RaftTransport raftTransport = transport != null ? transport : createDefaultTransport();
        this.node = new PartDbNode(config.nodeConfig(), raftTransport, storage);

        String selfAddress = config.peerAddresses().getOrDefault(
            config.nodeId(),
            config.nodeId() + ":" + config.grpcPort()
        );
        this.grpcServer = new GrpcServer(
            node,
            config.peerAddresses(),
            selfAddress,
            config.grpcServerConfig()
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
        grpcServer.start();
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .addKeyValue("grpcPort", config.grpcPort())
            .addKeyValue("raftPort", config.raftPort())
            .log("PartDB server started");
    }

    @Override
    public void close() {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Shutting down PartDB server");
        grpcServer.close();
        node.close();
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("PartDB server shut down");
    }
}
