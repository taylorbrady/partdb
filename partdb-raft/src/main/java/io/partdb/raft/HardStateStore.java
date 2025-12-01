package io.partdb.raft;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

public final class HardStateStore {
    private static final int MAGIC = 0x52414653;
    private static final int VERSION = 1;

    private final Path statePath;
    private final ReentrantLock lock;
    private volatile HardState cached;

    private HardStateStore(Path dataDirectory) {
        this.statePath = dataDirectory.resolve("hard-state.dat");
        this.lock = new ReentrantLock();
        this.cached = load();
    }

    public static HardStateStore open(Path dataDirectory) throws IOException {
        Files.createDirectories(dataDirectory);
        return new HardStateStore(dataDirectory);
    }

    public void save(HardState state) {
        lock.lock();
        try {
            var tempPath = statePath.resolveSibling("hard-state.tmp");

            try (var channel = FileChannel.open(tempPath,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {

                byte[] serialized = serialize(state);
                ByteBuffer buffer = ByteBuffer.wrap(serialized);
                while (buffer.hasRemaining()) {
                    channel.write(buffer);
                }
                channel.force(true);
            }

            Files.move(tempPath, statePath,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);

            fsyncDirectory(statePath.getParent());

            cached = state;

        } catch (IOException e) {
            throw new RaftException.MetadataException("Failed to save hard state", e);
        } finally {
            lock.unlock();
        }
    }

    public HardState load() {
        if (cached != null) return cached;

        lock.lock();
        try {
            if (!Files.exists(statePath)) {
                return HardState.INITIAL;
            }

            byte[] bytes = Files.readAllBytes(statePath);
            return deserialize(bytes);

        } catch (IOException e) {
            throw new RaftException.MetadataException("Failed to load hard state", e);
        } finally {
            lock.unlock();
        }
    }

    private byte[] serialize(HardState state) {
        byte[] votedForBytes = state.votedFor() != null
            ? state.votedFor().getBytes(StandardCharsets.UTF_8)
            : new byte[0];

        int size = 4 + 4 + 8 + 4 + votedForBytes.length + 4;
        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putInt(MAGIC);
        buffer.putInt(VERSION);
        buffer.putLong(state.term());
        buffer.putInt(votedForBytes.length);

        if (votedForBytes.length > 0) {
            buffer.put(votedForBytes);
        }

        CRC32C crc = new CRC32C();
        crc.update(buffer.array(), 0, buffer.position());
        buffer.putInt((int) crc.getValue());

        return buffer.array();
    }

    private HardState deserialize(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);

        int magic = buffer.getInt();
        if (magic != MAGIC) {
            throw new RaftException.MetadataException(
                "Invalid hard state file: bad magic number 0x%08X".formatted(magic)
            );
        }

        int version = buffer.getInt();
        if (version != VERSION) {
            throw new RaftException.MetadataException(
                "Unsupported hard state version: %d".formatted(version)
            );
        }

        int checksumPos = buffer.limit() - 4;
        int expectedChecksum = buffer.getInt(checksumPos);

        var crc = new CRC32C();
        crc.update(buffer.array(), 0, checksumPos);

        if ((int) crc.getValue() != expectedChecksum) {
            throw new RaftException.MetadataException("Hard state checksum mismatch - file corrupted");
        }

        long term = buffer.getLong();
        int votedForLength = buffer.getInt();

        String votedFor = null;
        if (votedForLength > 0) {
            byte[] votedForBytes = new byte[votedForLength];
            buffer.get(votedForBytes);
            votedFor = new String(votedForBytes, StandardCharsets.UTF_8);
        }

        return new HardState(term, votedFor);
    }

    private void fsyncDirectory(Path directory) throws IOException {
        try (var channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException e) {
        }
    }
}
