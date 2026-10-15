package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class InMemoryRaftLog implements StableLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private HardState hardState = HardState.INITIAL;
    private SnapshotMetadata snapshot;

    @Override
    public long firstIndex() {
        return snapshot != null ? snapshot.index() + 1 : 1;
    }

    @Override
    public long lastIndex() {
        if (!entries.isEmpty()) {
            return entries.getLast().index();
        }
        return snapshot != null ? snapshot.index() : 0;
    }

    @Override
    public long term(long index) {
        if (snapshot != null && index == snapshot.index()) {
            return snapshot.term();
        }
        if (index < firstIndex() || index > lastIndex()) {
            throw new IllegalArgumentException("index " + index + " is outside [" + firstIndex() + ", " + lastIndex() + "]");
        }
        return entries.get(position(index)).term();
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        if (fromIndex < firstIndex()) {
            throw new IllegalArgumentException("index " + fromIndex + " is compacted");
        }
        var result = new ArrayList<LogEntry>();
        long bytes = 0;
        for (long index = fromIndex; index < Math.min(toIndex, lastIndex() + 1); index++) {
            var entry = entries.get(position(index));
            if (!result.isEmpty() && bytes + entry.payloadSize() > maxBytes) {
                break;
            }
            result.add(entry);
            bytes += entry.payloadSize();
        }
        return result;
    }

    HardState hardState() {
        return hardState;
    }

    Optional<SnapshotMetadata> snapshot() {
        return Optional.ofNullable(snapshot);
    }

    List<LogEntry> entries() {
        return List.copyOf(entries);
    }

    void persistHardState(HardState state) {
        hardState = state;
    }

    void persist(Ready ready) {
        ready.snapshot().ifPresent(this::installSnapshot);
        ready.hardState().ifPresent(state -> hardState = state);
        append(ready.entries());
    }

    void append(List<LogEntry> newEntries) {
        if (newEntries.isEmpty()) {
            return;
        }
        long first = newEntries.getFirst().index();
        if (first > lastIndex() + 1) {
            throw new IllegalArgumentException("append leaves a gap before index " + first);
        }
        entries.removeIf(entry -> entry.index() >= first);
        entries.addAll(newEntries);
    }

    void compact(SnapshotMetadata metadata) {
        snapshot = metadata;
        entries.removeIf(entry -> entry.index() <= metadata.index());
    }

    void installSnapshot(SnapshotMetadata metadata) {
        snapshot = metadata;
        entries.clear();
    }

    private int position(long index) {
        return Math.toIntExact(index - entries.getFirst().index());
    }
}
