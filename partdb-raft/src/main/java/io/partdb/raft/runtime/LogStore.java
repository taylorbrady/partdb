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
    private final List<LogEntry> entries;
    private SnapshotMetadata base;
    private HardState hardState;
    private long retainedBytes;

    private LogStore(WriteAheadLog wal) {
        this.wal = wal;
        var replay = wal.replay();
        this.hardState = replay.hardState();
        this.base = replay.base().orElse(null);
        long baseIndex = firstIndex() - 1;
        var live = replay.entries().stream().filter(entry -> entry.index() > baseIndex).toList();
        if (!live.isEmpty() && live.getFirst().index() != baseIndex + 1) {
            throw new RaftStorageException.Corruption("WAL resumes at " + live.getFirst().index() + " after log base " + baseIndex);
        }
        this.entries = new ArrayList<>(live);
        this.retainedBytes = entries.stream().mapToLong(LogEntry::payloadSize).sum();
    }

    static LogStore open(Path directory) {
        return new LogStore(WriteAheadLog.open(directory.resolve("wal"), SEGMENT_BYTES));
    }

    boolean isEmpty() {
        return base == null && entries.isEmpty() && hardState.equals(HardState.INITIAL);
    }

    HardState hardState() {
        return hardState;
    }

    Optional<SnapshotMetadata> base() {
        return Optional.ofNullable(base);
    }

    long retainedEntries() {
        return entries.size();
    }

    long retainedBytes() {
        return retainedBytes;
    }

    @Override
    public long firstIndex() {
        return base == null ? 1 : base.index() + 1;
    }

    @Override
    public long lastIndex() {
        return entries.isEmpty() ? firstIndex() - 1 : entries.getLast().index();
    }

    @Override
    public long term(long index) {
        if (base != null && index == base.index()) {
            return base.term();
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

    long compactionIndex(long durableIndex, LogRetention retention) {
        long index = Math.min(durableIndex, lastIndex());
        long kept = 0;
        long bytes = 0;
        while (index >= firstIndex() && kept < retention.entries()) {
            long size = entries.get(position(index)).payloadSize();
            if (bytes + size > retention.bytes()) {
                break;
            }
            bytes += size;
            kept++;
            index--;
        }
        return Math.max(index, firstIndex() - 1);
    }

    void compact(SnapshotMetadata next) {
        if (next.index() < firstIndex() || next.index() > lastIndex() || term(next.index()) != next.term()) {
            throw new IllegalArgumentException("log base " + next.index() + " does not match the log [" + firstIndex() + ", " + lastIndex() + "]");
        }
        wal.compact(next);
        var compacted = entries.subList(0, position(next.index() + 1));
        retainedBytes -= compacted.stream().mapToLong(LogEntry::payloadSize).sum();
        compacted.clear();
        base = next;
    }

    void reset(SnapshotMetadata next) {
        wal.reset(next);
        base = next;
        entries.clear();
        retainedBytes = 0;
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
