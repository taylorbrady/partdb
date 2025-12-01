package io.partdb.raft;

import io.partdb.common.statemachine.StateMachine;
import io.partdb.common.statemachine.StateSnapshot;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.zip.CRC32C;

public final class Snapshotter {
    private static final int MAGIC_NUMBER = 0x52534E50;
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 4 + 4 + 8 + 8 + 8 + 4; // magic + version + lastIncludedIndex + lastIncludedTerm + lastAppliedIndex + dataLength
    private static final String SNAPSHOT_FILENAME = "snapshot.dat";
    private static final String SNAPSHOT_TEMP_FILENAME = "snapshot.tmp";

    private final Path snapshotDirectory;
    private final StateMachine stateMachine;
    private final RaftLog log;

    public Snapshotter(Path snapshotDirectory, StateMachine stateMachine, RaftLog log) {
        this.snapshotDirectory = snapshotDirectory;
        this.stateMachine = stateMachine;
        this.log = log;
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
        Path snapshotPath = snapshotDirectory.resolve(SNAPSHOT_FILENAME);

        if (!Files.exists(snapshotPath)) {
            return Optional.empty();
        }

        try (FileChannel channel = FileChannel.open(snapshotPath, StandardOpenOption.READ);
             Arena arena = Arena.ofConfined()) {

            long fileSize = channel.size();
            MemorySegment mapped = channel.map(FileChannel.MapMode.READ_ONLY, 0, fileSize, arena);
            return Optional.of(deserializeSnapshot(mapped));

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

                byte[] stateData = snapshot.stateSnapshot().data();

                ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
                header.putInt(MAGIC_NUMBER);
                header.putInt(VERSION);
                header.putLong(snapshot.lastIncludedIndex());
                header.putLong(snapshot.lastIncludedTerm());
                header.putLong(snapshot.stateSnapshot().lastAppliedIndex());
                header.putInt(stateData.length);
                header.flip();
                while (header.hasRemaining()) {
                    channel.write(header);
                }

                ByteBuffer dataBuffer = ByteBuffer.wrap(stateData);
                while (dataBuffer.hasRemaining()) {
                    channel.write(dataBuffer);
                }

                CRC32C crc = new CRC32C();
                crc.update(stateData);

                ByteBuffer trailer = ByteBuffer.allocate(12);
                trailer.putLong(snapshot.stateSnapshot().checksum());
                trailer.putInt((int) crc.getValue());
                trailer.flip();
                while (trailer.hasRemaining()) {
                    channel.write(trailer);
                }

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

    private RaftSnapshot deserializeSnapshot(MemorySegment mapped) {
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
}
