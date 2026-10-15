package io.partdb.raft.runtime;

import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.SnapshotMetadata;
import io.partdb.raft.StableLog;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class LogStore implements StableLog, AutoCloseable {

    private static final long SEGMENT_BYTES = 64L << 20;

    private final WriteAheadLog wal;
    private final SnapshotStore snapshots;
    private final List<LogEntry> entries;
    private SnapshotStore.SnapshotFile snapshot;
    private HardState hardState;
    private long retainedBytes;

    private LogStore(WriteAheadLog wal, SnapshotStore snapshots) {
        this.wal = wal;
        this.snapshots = snapshots;
        this.snapshot = snapshots.latest().orElse(null);
        var replay = wal.replay();
        this.hardState = replay.hardState();
        this.entries = new ArrayList<>(continuation(replay));
        this.retainedBytes = entries.stream().mapToLong(LogEntry::payloadSize).sum();
    }

    static LogStore open(Path directory) {
        return new LogStore(WriteAheadLog.open(directory.resolve("wal"), SEGMENT_BYTES), SnapshotStore.open(directory.resolve("snap")));
    }

    private List<LogEntry> continuation(WriteAheadLog.Replay replay) {
        var replayed = replay.entries();
        if (snapshot == null) {
            if (!replayed.isEmpty() && replayed.getFirst().index() != 1) {
                throw new RaftStorageException.Corruption("WAL starts at " + replayed.getFirst().index() + " without a snapshot");
            }
            return replayed;
        }
        var metadata = snapshot.metadata();
        long index = metadata.index();
        boolean resetHere = replay.resetIndex().isPresent() && replay.resetIndex().getAsLong() == index;
        boolean startsAfter = !replayed.isEmpty() && replayed.getFirst().index() == index + 1;
        boolean containsSnapshotEntry = replayed.stream().anyMatch(entry -> entry.index() == index && entry.term() == metadata.term());
        if (resetHere || startsAfter || containsSnapshotEntry) {
            return replayed.stream().filter(entry -> entry.index() > index).toList();
        }
        wal.reset(index, metadata.term());
        return List.of();
    }

    boolean isEmpty() {
        return snapshot == null && entries.isEmpty() && hardState.equals(HardState.INITIAL);
    }

    HardState hardState() {
        return hardState;
    }

    Optional<SnapshotStore.SnapshotFile> snapshot() {
        return Optional.ofNullable(snapshot);
    }

    SnapshotStore.Lease lease(SnapshotStore.SnapshotFile file) {
        return snapshots.lease(file);
    }

    long retainedBytes() {
        return retainedBytes;
    }

    @Override
    public long firstIndex() {
        return snapshot == null ? 1 : snapshot.metadata().index() + 1;
    }

    @Override
    public long lastIndex() {
        return entries.isEmpty() ? firstIndex() - 1 : entries.getLast().index();
    }

    @Override
    public long term(long index) {
        if (snapshot != null && index == snapshot.metadata().index()) {
            return snapshot.metadata().term();
        }
        return entries.get(position(index)).term();
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        int from = position(fromIndex);
        int to = Math.min(entries.size(), position(Math.max(fromIndex, toIndex)));
        var result = new ArrayList<LogEntry>();
        long bytes = 0;
        for (int i = from; i < to; i++) {
            var entry = entries.get(i);
            if (!result.isEmpty() && bytes + entry.payloadSize() > maxBytes) {
                break;
            }
            result.add(entry);
            bytes += entry.payloadSize();
        }
        return result;
    }

    void append(Optional<HardState> state, List<LogEntry> newEntries) {
        if (!newEntries.isEmpty()) {
            long first = newEntries.getFirst().index();
            if (first < firstIndex() || first > lastIndex() + 1) {
                throw new IllegalArgumentException("cannot append index " + first + " to log [" + firstIndex() + ", " + lastIndex() + "]");
            }
            var truncated = entries.subList(position(first), entries.size());
            retainedBytes -= truncated.stream().mapToLong(LogEntry::payloadSize).sum();
            truncated.clear();
            entries.addAll(newEntries);
            retainedBytes += newEntries.stream().mapToLong(LogEntry::payloadSize).sum();
        }
        wal.append(state, newEntries);
        state.ifPresent(next -> hardState = next);
    }

    void sync() {
        wal.sync();
    }

    Path stagingFile() {
        return snapshots.stagingFile();
    }

    void installSnapshot(SnapshotMetadata metadata, Path staged, int checksum) {
        snapshot = snapshots.install(metadata, staged, checksum);
        wal.reset(metadata.index(), metadata.term());
        entries.clear();
        retainedBytes = 0;
    }

    void compact(SnapshotMetadata metadata, Path staged, int checksum) {
        if (metadata.index() > lastIndex() || term(metadata.index()) != metadata.term()) {
            throw new IllegalArgumentException("snapshot " + metadata.index() + " does not match the log");
        }
        int keepFrom = position(metadata.index() + 1);
        snapshot = snapshots.install(metadata, staged, checksum);
        var compacted = entries.subList(0, keepFrom);
        retainedBytes -= compacted.stream().mapToLong(LogEntry::payloadSize).sum();
        compacted.clear();
        wal.compact(metadata.index());
    }

    @Override
    public void close() {
        wal.close();
    }

    private int position(long index) {
        long position = index - firstIndex();
        if (position < 0) {
            throw new IllegalArgumentException("index " + index + " is compacted; the log starts at " + firstIndex());
        }
        return Math.toIntExact(Math.min(position, entries.size()));
    }
}
