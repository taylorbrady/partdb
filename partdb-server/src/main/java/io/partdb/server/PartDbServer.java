package io.partdb.server;

import io.partdb.consensus.ConsensusRuntimeFactory;
import io.partdb.consensus.RaftConsensusRuntimeFactory;
import io.partdb.node.PartDbNode;
import io.partdb.transport.grpc.GrpcRaftTransport;
import io.partdb.transport.grpc.GrpcServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public final class PartDbServer implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PartDbServer.class);

    private final PartDbServerConfig config;
    private final ConsensusRuntimeFactory runtimeFactory;
    private PartDbNode node;
    private GrpcServer grpcServer;
    private JmxRegistrations jmxRegistrations;
    private AdminHttpServer adminHttpServer;

    public PartDbServer(PartDbServerConfig config) {
        this(config, null);
    }

    PartDbServer(PartDbServerConfig config, ConsensusRuntimeFactory runtimeFactory) {
        this.config = Objects.requireNonNull(config, "config must not be null");
        this.runtimeFactory = runtimeFactory;
    }

    private ConsensusRuntimeFactory createDefaultRuntimeFactory() {
        return new RaftConsensusRuntimeFactory(
            () -> new GrpcRaftTransport(
                config.nodeId(),
                config.raftPort(),
                config.raftPeerAddresses()
            )
        );
    }

    public void start() throws IOException {
        if (node != null) {
            throw new IllegalStateException("PartDB server already started");
        }
        log.atInfo()
            .addKeyValue("nodeId", config.nodeId())
            .log("Starting PartDB server");
        try {
            ConsensusRuntimeFactory consensusRuntimeFactory = runtimeFactory != null
                ? runtimeFactory
                : createDefaultRuntimeFactory();
            node = PartDbNode.open(config.nodeConfig(), consensusRuntimeFactory);
            grpcServer = new GrpcServer(node, config.grpcPort());
            jmxRegistrations = new JmxRegistrations(node);
            grpcServer.start();
            adminHttpServer = new AdminHttpServer(node, config.adminPort());
            adminHttpServer.start();
            jmxRegistrations.register();
            log.atInfo()
                .addKeyValue("nodeId", config.nodeId())
                .addKeyValue("grpcPort", config.grpcPort())
                .addKeyValue("raftPort", config.raftPort())
                .addKeyValue("adminPort", config.adminPort())
                .log("PartDB server started");
        } catch (Exception e) {
            close();
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
                if (grpcServer != null) {
                    try {
                        grpcServer.close();
                    } finally {
                        grpcServer = null;
                    }
                }
            } finally {
                try {
                    if (jmxRegistrations != null) {
                        try {
                            jmxRegistrations.close();
                        } finally {
                            jmxRegistrations = null;
                        }
                    }
                } finally {
                    if (node != null) {
                        try {
                            node.close();
                        } finally {
                            node = null;
                        }
                    }
                    log.atInfo()
                        .addKeyValue("nodeId", config.nodeId())
                        .log("PartDB server shut down");
                }
            }
        }
    }
}
