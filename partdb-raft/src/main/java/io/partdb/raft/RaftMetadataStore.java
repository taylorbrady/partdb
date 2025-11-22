package io.partdb.raft;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

public final class RaftMetadataStore implements AutoCloseable {
    private static final int MAGIC = 0x5241464D;
    private static final int VERSION = 1;

    private final Path metadataPath;
    private final ReentrantLock lock;
    private final Arena arena;
    private volatile RaftMetadata cached;

    private RaftMetadataStore(Path dataDirectory) throws IOException {
        this.metadataPath = dataDirectory.resolve("raft-metadata.dat");
        this.lock = new ReentrantLock();
        this.arena = Arena.ofConfined();
        this.cached = load();
    }

    public static RaftMetadataStore open(Path dataDirectory) throws IOException {
        Files.createDirectories(dataDirectory);
        return new RaftMetadataStore(dataDirectory);
    }

    public void save(RaftMetadata metadata) {
        lock.lock();
        try {
            var tempPath = metadataPath.resolveSibling("raft-metadata.tmp");

            try (var channel = FileChannel.open(tempPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                byte[] serialized = serialize(metadata);
                channel.write(ByteBuffer.wrap(serialized));
                channel.force(true);
            }

            Files.move(tempPath, metadataPath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);

            fsyncDirectory(metadataPath.getParent());

            cached = metadata;

        } catch (IOException e) {
            throw new RaftException.MetadataException("Failed to save metadata", e);
        } finally {
            lock.unlock();
        }
    }

    public RaftMetadata load() {
        if (cached != null) return cached;

        lock.lock();
        try {
            if (!Files.exists(metadataPath)) {
                return RaftMetadata.initial();
            }

            byte[] bytes = Files.readAllBytes(metadataPath);
            return deserialize(bytes);

        } catch (IOException e) {
            throw new RaftException.MetadataException("Failed to load metadata", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        arena.close();
    }

    private byte[] serialize(RaftMetadata metadata) {
        byte[] votedForBytes = metadata.votedFor() != null
            ? metadata.votedFor().getBytes(StandardCharsets.UTF_8)
            : new byte[0];

        long size = 4 + 4 + 8 + 4 + votedForBytes.length + 4;
        MemorySegment buffer = arena.allocate(size);
        long offset = 0;

        buffer.set(ValueLayout.JAVA_INT, offset, MAGIC);
        offset += 4;
        buffer.set(ValueLayout.JAVA_INT, offset, VERSION);
        offset += 4;
        buffer.set(ValueLayout.JAVA_LONG, offset, metadata.currentTerm());
        offset += 8;
        buffer.set(ValueLayout.JAVA_INT, offset, votedForBytes.length);
        offset += 4;

        if (votedForBytes.length > 0) {
            MemorySegment.copy(votedForBytes, 0, buffer, ValueLayout.JAVA_BYTE, offset, votedForBytes.length);
            offset += votedForBytes.length;
        }

        CRC32C crc = new CRC32C();
        for (long i = 0; i < offset; i++) {
            crc.update(buffer.get(ValueLayout.JAVA_BYTE, i));
        }
        buffer.set(ValueLayout.JAVA_INT, offset, (int) crc.getValue());

        return buffer.toArray(ValueLayout.JAVA_BYTE);
    }

    private RaftMetadata deserialize(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);

        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new RaftException.MetadataException(
                "Invalid metadata file: bad magic number 0x%08X".formatted(magic)
            );
        }

        int version = buffer.getInt();
        if (version != VERSION) {
            throw new RaftException.MetadataException(
                "Unsupported metadata version: %d".formatted(version)
            );
        }

        int checksumPos = buffer.limit() - 4;
        int expectedChecksum = buffer.getInt(checksumPos);

        var crc = new CRC32C();
        crc.update(buffer.array(), 0, checksumPos);

        if ((int) crc.getValue() != expectedChecksum) {
            throw new RaftException.MetadataException("Metadata checksum mismatch - file corrupted");
        }

        long currentTerm = buffer.getLong();
        int votedForLength = buffer.getInt();

        String votedFor = null;
        if (votedForLength > 0) {
            byte[] votedForBytes = new byte[votedForLength];
            buffer.get(votedForBytes);
            votedFor = new String(votedForBytes, StandardCharsets.UTF_8);
        }

        return new RaftMetadata(currentTerm, votedFor);
    }

    private void fsyncDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
        }
    }
}
