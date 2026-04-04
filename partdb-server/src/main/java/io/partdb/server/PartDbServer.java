package io.partdb.server;

import io.partdb.node.PartDbNode;
import io.partdb.node.replication.ReplicationTransport;
import io.partdb.transport.grpc.GrpcReplicationTransport;
import io.partdb.transport.grpc.GrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final PartDbNode node;
    private final GrpcServer grpcServer;
    private final JmxRegistrations jmxRegistrations;
    private AdminHttpServer adminHttpServer;

    public PartDbServer(PartDbServerConfig config) {
        this(config, null);
    }

    PartDbServer(PartDbServerConfig config, ReplicationTransport transport) {
        this.config = config;
        ReplicationTransport replicationTransport = transport != null ? transport : createDefaultTransport();
        this.node = PartDbNode.open(config.nodeConfig(), replicationTransport);
        this.grpcServer = new GrpcServer(
            node,
            config.raftPeerAddresses(),
            config.selfRaftEndpoint().toString(),
            config.grpcPort()
        );
        this.jmxRegistrations = new JmxRegistrations(node);
    }

    private ReplicationTransport createDefaultTransport() {
        return new GrpcReplicationTransport(
            config.nodeId(),
            config.raftPort(),
            config.raftPeerAddresses()
        );
    }

    public void start() throws IOException {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Starting PartDB server");
        boolean grpcStarted = false;
        boolean jmxRegistered = false;
        try {
            grpcServer.start();
            grpcStarted = true;
            adminHttpServer = new AdminHttpServer(node, config.adminPort());
            adminHttpServer.start();
            jmxRegistrations.register();
            jmxRegistered = true;
            log.atInfo()
                .addKeyValue("nodeId", config.nodeId())
                .addKeyValue("grpcPort", config.grpcPort())
                .addKeyValue("raftPort", config.raftPort())
                .addKeyValue("adminPort", config.adminPort())
                .log("PartDB server started");
        } catch (Exception e) {
            if (jmxRegistered) {
                jmxRegistrations.close();
            }
            if (adminHttpServer != null) {
                try {
                    adminHttpServer.close();
                } finally {
                    adminHttpServer = null;
                }
            }
            if (grpcStarted) {
                grpcServer.close();
            }
            if (e instanceof IOException ioException) {
                throw ioException;
            }
            if (e instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IOException("Failed to start PartDB server", e);
        }
    }

    @Override
    public void close() {
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Shutting down PartDB server");
        try {
            if (adminHttpServer != null) {
                try {
                    adminHttpServer.close();
                } finally {
                    adminHttpServer = null;
                }
            }
        } finally {
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
}
