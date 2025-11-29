package io.partdb.server;

import io.partdb.raft.RaftNode;
import io.partdb.raft.transport.GrpcRaftServer;
import io.partdb.raft.transport.GrpcRaftTransport;
import io.partdb.server.grpc.KvServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public final class PartDb implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(PartDb.class);

    private final PartDbConfig config;
    private final Database database;
    private final GrpcRaftTransport raftTransport;
    private final RaftNode raftNode;
    private final GrpcRaftServer raftServer;
    private final Lessor lessor;
    private final KvServer kvServer;

    public PartDb(PartDbConfig config) {
        this.config = config;
        this.database = Database.open(
            config.dataDirectory().resolve("db"),
            config.storeConfig()
        );

        this.raftTransport = new GrpcRaftTransport(config.raftTransportConfig());

        this.raftNode = new RaftNode(
            config.raftConfig(),
            database,
            raftTransport
        );

        this.raftServer = new GrpcRaftServer(raftNode, config.raftTransportConfig());

        this.lessor = new Lessor(raftNode, database.leases());

        this.kvServer = new KvServer(raftNode, database, lessor, config.kvServerConfig());
    }

    public void start() throws IOException {
        logger.info("Starting PartDB server...");
        raftServer.start();
        kvServer.start();
        lessor.start();
        logger.info("PartDB server started (raft port: {}, kv port: {})",
            config.raftTransportConfig().bindPort(), config.kvServerConfig().port());
    }

    public Database database() {
        return database;
    }

    public RaftNode raftNode() {
        return raftNode;
    }

    public Lessor lessor() {
        return lessor;
    }

    @Override
    public void close() {
        logger.info("Shutting down PartDB server...");
        lessor.close();
        kvServer.close();
        raftServer.close();
        raftTransport.close();
        raftNode.close();
        database.close();
        logger.info("PartDB server shut down");
    }
}
