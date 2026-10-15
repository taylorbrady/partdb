package io.partdb.raft.runtime;

import io.partdb.raft.Membership;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32C;

import static io.partdb.raft.runtime.BinaryFormat.ORDER;

record ClusterFile(ClusterId clusterId, Membership initialMembership) {

    private static final int MAGIC = 0x52545343;
    private static final int VERSION = 1;
    private static final String NAME = "cluster";

    static Optional<ClusterFile> read(Path directory) {
        Path path = directory.resolve(NAME);
        if (!Files.exists(path)) {
            return Optional.empty();
        }
        try {
            var buffer = ByteBuffer.wrap(Files.readAllBytes(path)).order(ORDER);
            int bodyLength = buffer.remaining() - Integer.BYTES;
            if (bodyLength < 0) {
                throw new RaftStorageException.Corruption("truncated cluster file " + path);
            }
            var crc = new CRC32C();
            crc.update(buffer.slice(0, bodyLength));
            if ((int) crc.getValue() != buffer.getInt(bodyLength)) {
                throw new RaftStorageException.Corruption("cluster file checksum mismatch in " + path);
            }
            if (buffer.getInt() != MAGIC || buffer.getInt() != VERSION) {
                throw new RaftStorageException.Corruption("unsupported cluster file " + path);
            }
            var clusterId = new ClusterId(BinaryFormat.getString(buffer));
            var membership = BinaryFormat.getMembership(buffer);
            return Optional.of(new ClusterFile(clusterId, membership));
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to read cluster file " + path, e);
        } catch (BufferUnderflowException | IllegalArgumentException e) {
            throw new RaftStorageException.Corruption("malformed cluster file " + path + ": " + e.getMessage());
        }
    }

    void write(Path directory) {
        int bodyLength = 2 * Integer.BYTES + BinaryFormat.stringSize(clusterId.value()) + BinaryFormat.membershipSize(initialMembership);
        var buffer = ByteBuffer.allocate(bodyLength + Integer.BYTES).order(ORDER);
        buffer.putInt(MAGIC).putInt(VERSION);
        BinaryFormat.putString(buffer, clusterId.value());
        BinaryFormat.putMembership(buffer, initialMembership);
        var crc = new CRC32C();
        crc.update(buffer.slice(0, bodyLength));
        buffer.putInt((int) crc.getValue()).flip();

        Path staging = directory.resolve(NAME + ".tmp");
        try {
            try (var channel = FileChannel.open(staging, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }
            Files.move(staging, directory.resolve(NAME), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Fsync.directory(directory);
        } catch (IOException e) {
            throw new RaftStorageException.IO("failed to write cluster file in " + directory, e);
        }
    }
}
