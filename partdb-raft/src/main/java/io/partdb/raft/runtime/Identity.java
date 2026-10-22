package io.partdb.raft.runtime;

import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.zip.CRC32C;

import static io.partdb.raft.runtime.BinaryFormat.ORDER;

record Identity(ClusterId clusterId, NodeId nodeId, Membership initialMembership) {

    private static final int MAGIC = 0x544E4449;
    private static final int VERSION = 1;
    private static final String FILE_NAME = "IDENTITY";

    Identity {
        Objects.requireNonNull(clusterId, "clusterId must not be null");
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(initialMembership, "initialMembership must not be null");
    }

    static Identity open(Path directory, RaftConfig config, LogStore log) {
        var recorded = read(directory);
        if (recorded.isEmpty()) {
            if (!log.isEmpty()) {
                throw new RaftStorageException.Corruption("raft directory " + directory + " has a log but no identity");
            }
            var bootstrap = config.bootstrap();
            var created = new Identity(bootstrap.clusterId(), config.nodeId(), bootstrap.membership());
            created.write(directory);
            return created;
        }
        var identity = recorded.get();
        if (!identity.nodeId().equals(config.nodeId())) {
            throw new IllegalStateException("raft directory " + directory + " belongs to node " + identity.nodeId() + ", not " + config.nodeId());
        }
        return identity;
    }

    private static Optional<Identity> read(Path directory) {
        Path path = directory.resolve(FILE_NAME);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            var buffer = ByteBuffer.wrap(Files.readAllBytes(path)).order(ORDER);
            int bodyLength = buffer.remaining() - Integer.BYTES;
            if (bodyLength < 0) {
                throw new RaftStorageException.Corruption("truncated identity file " + path);
            }
            var crc = new CRC32C();
            crc.update(buffer.slice(0, bodyLength));
            if ((int) crc.getValue() != buffer.getInt(bodyLength)) {
                throw new RaftStorageException.Corruption("identity file checksum mismatch in " + path);
            }
            if (buffer.getInt() != MAGIC || buffer.getInt() != VERSION) {
                throw new RaftStorageException.Corruption("unsupported identity file " + path);
            }
            var clusterId = new ClusterId(BinaryFormat.getString(buffer));
            var nodeId = BinaryFormat.getNode(buffer);
            var membership = BinaryFormat.getMembership(buffer);
            return Optional.of(new Identity(clusterId, nodeId, membership));
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to read identity file " + path, e);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new RaftStorageException.Corruption("malformed identity file " + path + ": " + e.getMessage());
        }
    }

    private void write(Path directory) {
        int bodyLength = 2 * Integer.BYTES
            + BinaryFormat.stringSize(clusterId.value())
            + BinaryFormat.nodeSize(nodeId)
            + BinaryFormat.membershipSize(initialMembership);
        var buffer = ByteBuffer.allocate(bodyLength + Integer.BYTES).order(ORDER);
        buffer.putInt(MAGIC).putInt(VERSION);
        BinaryFormat.putString(buffer, clusterId.value());
        BinaryFormat.putNode(buffer, nodeId);
        BinaryFormat.putMembership(buffer, initialMembership);
        var crc = new CRC32C();
        crc.update(buffer.slice(0, bodyLength));
        buffer.putInt((int) crc.getValue()).flip();
        Disk.writeAtomically(directory.resolve(FILE_NAME), buffer);
    }
}
