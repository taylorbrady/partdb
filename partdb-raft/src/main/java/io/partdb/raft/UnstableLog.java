package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

final class UnstableLog {
    private final List<LogEntry> entries = new ArrayList<>();
    private SnapshotMetadata snapshot;
    private boolean snapshotTaken;
    private long offset;
    private long takenThrough;

    UnstableLog(long offset) {
        this.offset = offset;
        this.takenThrough = offset - 1;
    }

    long offset() {
        return offset;
    }

    boolean hasEntries() {
        return !entries.isEmpty();
    }

    Optional<SnapshotMetadata> snapshot() {
        return Optional.ofNullable(snapshot);
    }

    OptionalLong lastIndex() {
        if (!entries.isEmpty()) {
            return OptionalLong.of(offset + entries.size() - 1);
        }
        if (snapshot != null) {
            return OptionalLong.of(snapshot.index());
        }
        return OptionalLong.empty();
    }

    OptionalLong term(long index) {
        if (snapshot != null && index == snapshot.index()) {
            return OptionalLong.of(snapshot.term());
        }
        if (index < offset || index >= offset + entries.size()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(entries.get(position(index)).term());
    }

    List<LogEntry> slice(long fromIndex, long toIndex) {
        long from = Math.max(fromIndex, offset);
        long to = Math.min(toIndex, offset + entries.size());
        if (from >= to) {
            return List.of();
        }
        return List.copyOf(entries.subList(position(from), position(to)));
    }

    void append(LogEntry entry) {
        long index = entry.index();
        long end = offset + entries.size();
        if (index == end) {
            entries.add(entry);
            return;
        }
        if (index > end) {
            throw new IllegalStateException("unstable log gap: expected index " + end + " but got " + index);
        }
        if (index < offset) {
            entries.clear();
            offset = index;
        } else {
            entries.subList(position(index), entries.size()).clear();
        }
        entries.add(entry);
        takenThrough = Math.min(takenThrough, index - 1);
    }

    void restore(SnapshotMetadata metadata) {
        snapshot = metadata;
        snapshotTaken = false;
        entries.clear();
        offset = metadata.index() + 1;
        takenThrough = metadata.index();
    }

    void stableTo(long index, long term) {
        if (snapshot != null && snapshot.index() == index && snapshot.term() == term) {
            snapshot = null;
            return;
        }
        if (index < offset || index >= offset + entries.size() || entries.get(position(index)).term() != term) {
            return;
        }
        entries.subList(0, position(index) + 1).clear();
        offset = index + 1;
        snapshot = null;
    }

    Optional<SnapshotMetadata> takeSnapshot() {
        if (snapshot == null || snapshotTaken) {
            return Optional.empty();
        }
        snapshotTaken = true;
        return Optional.of(snapshot);
    }

    List<LogEntry> takeEntries() {
        var taken = slice(takenThrough + 1, offset + entries.size());
        takenThrough = offset + entries.size() - 1;
        return taken;
    }

    private int position(long index) {
        return Math.toIntExact(index - offset);
    }
}
