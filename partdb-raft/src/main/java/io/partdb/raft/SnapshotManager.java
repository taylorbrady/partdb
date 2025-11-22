package io.partdb.raft;

import io.partdb.common.statemachine.StateMachine;
import io.partdb.common.statemachine.StateSnapshot;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32C;

public final class SnapshotManager implements AutoCloseable {
    private static final int MAGIC_NUMBER = 0x52534E50;
    private static final int VERSION = 1;
    private static final String SNAPSHOT_FILENAME = "snapshot.dat";
    private static final String SNAPSHOT_TEMP_FILENAME = "snapshot.tmp";
    private static final long CHUNK_SIZE = 64 * 1024;

    private final Path snapshotDirectory;
    private final StateMachine stateMachine;
    private final RaftLog log;
    private final Arena arena;

    public SnapshotManager(Path snapshotDirectory, StateMachine stateMachine, RaftLog log) {
        this.snapshotDirectory = snapshotDirectory;
        this.stateMachine = stateMachine;
        this.log = log;
        this.arena = Arena.ofShared();
    }

    public RaftSnapshot createSnapshot(long lastIncludedIndex, long lastIncludedTerm) {
        try {
            StateSnapshot stateSnapshot = stateMachine.snapshot();

            RaftSnapshot raftSnapshot = new RaftSnapshot(
                lastIncludedIndex,
                lastIncludedTerm,
                stateSnapshot
            );

            saveSnapshot(raftSnapshot);

            log.deleteBefore(lastIncludedIndex);

            return raftSnapshot;
        } catch (Exception e) {
            throw new RaftException.SnapshotException("Failed to create snapshot", e);
        }
    }

    public void installSnapshot(RaftSnapshot snapshot) {
        try {
            stateMachine.restore(snapshot.stateSnapshot());
            saveSnapshot(snapshot);
            log.truncateAfter(snapshot.lastIncludedIndex());
        } catch (Exception e) {
            throw new RaftException.SnapshotException("Failed to install snapshot", e);
        }
    }

    public Optional<RaftSnapshot> loadLatestSnapshot() {
        try {
            Path snapshotPath = snapshotDirectory.resolve(SNAPSHOT_FILENAME);

            if (!Files.exists(snapshotPath)) {
                return Optional.empty();
            }

            try (FileChannel channel = FileChannel.open(snapshotPath, StandardOpenOption.READ)) {
                long fileSize = channel.size();
                MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
                return Optional.of(deserializeSnapshotFromMemory(mapped));
            }
        } catch (IOException e) {
            throw new RaftException.SnapshotException("Failed to load snapshot", e);
        }
    }

    private void saveSnapshot(RaftSnapshot snapshot) {
        try {
            Files.createDirectories(snapshotDirectory);

            Path tempPath = snapshotDirectory.resolve(SNAPSHOT_TEMP_FILENAME);
            Path finalPath = snapshotDirectory.resolve(SNAPSHOT_FILENAME);

            try (FileChannel channel = FileChannel.open(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

                MemorySegment header = serializeSnapshotHeaderOffHeap(snapshot);
                channel.write(header.asByteBuffer());

                byte[] stateData = snapshot.stateSnapshot().data();
                long remaining = stateData.length;
                long srcOffset = 0;

                while (remaining > 0) {
                    long chunkSize = Math.min(CHUNK_SIZE, remaining);
                    MemorySegment chunk = arena.allocate(chunkSize);
                    MemorySegment.copy(stateData, (int) srcOffset, chunk, ValueLayout.JAVA_BYTE, 0, (int) chunkSize);
                    channel.write(chunk.asByteBuffer());

                    srcOffset += chunkSize;
                    remaining -= chunkSize;
                }

                MemorySegment checksumSeg = arena.allocate(ValueLayout.JAVA_LONG);
                checksumSeg.set(ValueLayout.JAVA_LONG, 0, snapshot.stateSnapshot().checksum());
                channel.write(checksumSeg.asByteBuffer());

                CRC32C crc = new CRC32C();
                crc.update(stateData);
                MemorySegment crcSeg = arena.allocate(ValueLayout.JAVA_INT);
                crcSeg.set(ValueLayout.JAVA_INT, 0, (int) crc.getValue());
                channel.write(crcSeg.asByteBuffer());

                channel.force(true);
            }

            Files.move(tempPath, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

            try (FileChannel dirChannel = FileChannel.open(snapshotDirectory, StandardOpenOption.READ)) {
                dirChannel.force(true);
            } catch (IOException ignored) {
            }

        } catch (IOException e) {
            throw new RaftException.SnapshotException("Failed to save snapshot", e);
        }
    }

    private MemorySegment serializeSnapshotHeaderOffHeap(RaftSnapshot snapshot) {
        long headerSize = 4 + 4 + 8 + 8 + 8 + 4;
        MemorySegment seg = arena.allocate(headerSize);
        long offset = 0;

        seg.set(ValueLayout.JAVA_INT, offset, MAGIC_NUMBER);
        offset += 4;
        seg.set(ValueLayout.JAVA_INT, offset, VERSION);
        offset += 4;
        seg.set(ValueLayout.JAVA_LONG, offset, snapshot.lastIncludedIndex());
        offset += 8;
        seg.set(ValueLayout.JAVA_LONG, offset, snapshot.lastIncludedTerm());
        offset += 8;
        seg.set(ValueLayout.JAVA_LONG, offset, snapshot.stateSnapshot().lastAppliedIndex());
        offset += 8;
        seg.set(ValueLayout.JAVA_INT, offset, snapshot.stateSnapshot().data().length);

        return seg;
    }

    private RaftSnapshot deserializeSnapshotFromMemory(MemorySegment mapped) {
        long offset = 0;

        int magic = mapped.get(ValueLayout.JAVA_INT, offset);
        offset += 4;
        if (magic != MAGIC_NUMBER) {
            throw new RaftException.SnapshotException("Invalid snapshot magic number: " + Integer.toHexString(magic));
        }

        int version = mapped.get(ValueLayout.JAVA_INT, offset);
        offset += 4;
        if (version != VERSION) {
            throw new RaftException.SnapshotException("Unsupported snapshot version: " + version);
        }

        long lastIncludedIndex = mapped.get(ValueLayout.JAVA_LONG, offset);
        offset += 8;
        long lastIncludedTerm = mapped.get(ValueLayout.JAVA_LONG, offset);
        offset += 8;

        long lastAppliedIndex = mapped.get(ValueLayout.JAVA_LONG, offset);
        offset += 8;
        int dataLength = mapped.get(ValueLayout.JAVA_INT, offset);
        offset += 4;

        byte[] data = new byte[dataLength];
        MemorySegment.copy(mapped, ValueLayout.JAVA_BYTE, offset, data, 0, dataLength);
        offset += dataLength;

        long stateChecksum = mapped.get(ValueLayout.JAVA_LONG, offset);
        offset += 8;

        int expectedChecksum = mapped.get(ValueLayout.JAVA_INT, offset);

        CRC32C crc = new CRC32C();
        crc.update(data);
        int actualChecksum = (int) crc.getValue();

        if (actualChecksum != expectedChecksum) {
            throw new RaftException.SnapshotException("Snapshot checksum mismatch");
        }

        StateSnapshot stateSnapshot = StateSnapshot.restore(lastAppliedIndex, data, stateChecksum);

        return new RaftSnapshot(lastIncludedIndex, lastIncludedTerm, stateSnapshot);
    }

    @Override
    public void close() {
        arena.close();
    }
}
