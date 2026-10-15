package io.partdb.raft.runtime;

import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLog;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.SnapshotMetadata;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class FileRaftStorage implements RaftLog, AutoCloseable {

    private static final long SEGMENT_BYTES = 64L << 20;

    private final WriteAheadLog wal;
    private final SnapshotStore snapshots;
    private final List<RaftLogEntry> entries;
    private SnapshotStore.SnapshotFile snapshot;
    private RaftHardState hardState;

    private FileRaftStorage(WriteAheadLog wal, SnapshotStore snapshots) {
        this.wal = wal;
        this.snapshots = snapshots;
        this.snapshot = snapshots.latest().orElse(null);
        var replay = wal.replay();
        this.hardState = replay.hardState();
        this.entries = new ArrayList<>(continuation(replay));
    }

    static FileRaftStorage open(Path directory) {
        return new FileRaftStorage(WriteAheadLog.open(directory.resolve("wal"), SEGMENT_BYTES), SnapshotStore.open(directory.resolve("snap")));
    }

    private List<RaftLogEntry> continuation(WriteAheadLog.Replay replay) {
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

    RaftHardState hardState() {
        return hardState;
    }

    Optional<SnapshotStore.SnapshotFile> snapshot() {
        return Optional.ofNullable(snapshot);
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
    public List<RaftLogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        int from = position(fromIndex);
        int to = Math.min(entries.size(), position(Math.max(fromIndex, toIndex)));
        var result = new ArrayList<RaftLogEntry>();
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

    void append(Optional<RaftHardState> state, List<RaftLogEntry> newEntries) {
        if (!newEntries.isEmpty()) {
            long first = newEntries.getFirst().index();
            if (first < firstIndex() || first > lastIndex() + 1) {
                throw new IllegalArgumentException("cannot append index " + first + " to log [" + firstIndex() + ", " + lastIndex() + "]");
            }
            entries.subList(position(first), entries.size()).clear();
            entries.addAll(newEntries);
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

    void installSnapshot(SnapshotMetadata metadata, Path staged) {
        snapshot = snapshots.install(metadata, staged);
        wal.reset(metadata.index(), metadata.term());
        entries.clear();
    }

    void compact(SnapshotMetadata metadata, Path staged) {
        if (metadata.index() > lastIndex() || term(metadata.index()) != metadata.term()) {
            throw new IllegalArgumentException("snapshot " + metadata.index() + " does not match the log");
        }
        int keepFrom = position(metadata.index() + 1);
        snapshot = snapshots.install(metadata, staged);
        entries.subList(0, keepFrom).clear();
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
