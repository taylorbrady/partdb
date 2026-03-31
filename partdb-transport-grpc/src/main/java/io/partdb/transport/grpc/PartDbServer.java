package io.partdb.transport.grpc;

import io.partdb.node.PartDbNode;
import io.partdb.node.transport.ConsensusTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final PartDbNode node;
    private final GrpcServer grpcServer;
    private final JmxRegistrations jmxRegistrations;

    public PartDbServer(PartDbServerConfig config) {
        this(config, null);
    }

    PartDbServer(PartDbServerConfig config, ConsensusTransport transport) {
        this.config = config;
        ConsensusTransport raftTransport = transport != null ? transport : createDefaultTransport();
        this.node = new PartDbNode(config.nodeConfig(), raftTransport);
        this.grpcServer = new GrpcServer(
            node,
            config.raftPeerAddresses(),
            config.selfRaftEndpoint().toString(),
            config.grpcServerConfig()
        );
        this.jmxRegistrations = new JmxRegistrations(node);
    }

    private ConsensusTransport createDefaultTransport() {
        var transportConfig = GrpcConsensusTransportConfig.create(
            config.nodeId(),
            config.raftPort(),
            config.raftPeerAddresses()
        );
        return new GrpcConsensusTransport(transportConfig);
    }

    public void start() throws IOException {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Starting PartDB server");
        grpcServer.start();
        jmxRegistrations.register();
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
        try {
            grpcServer.close();
        } finally {
            try {
                jmxRegistrations.close();
            } finally {
                node.close();
                log.atInfo()
                    .addKeyValue("nodeId", config.nodeId())
                    .log("PartDB server shut down");
            }
        }
    }
}
