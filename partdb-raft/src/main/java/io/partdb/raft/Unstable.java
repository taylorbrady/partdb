package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;

final class Unstable {
    private final List<LogEntry> entries = new ArrayList<>();
    private long offset;
    private Snapshot snapshot;

    Unstable(long offset) {
        this.offset = offset;
    }

    void append(LogEntry entry) {
        long idx = entry.index();

        if (idx < offset) {
            return;
        }

        int arrayIdx = (int) (idx - offset);
        if (arrayIdx < entries.size()) {
            if (entries.get(arrayIdx).term() != entry.term()) {
                entries.subList(arrayIdx, entries.size()).clear();
                entries.add(entry);
            }
        } else {
            entries.add(entry);
        }
    }

    void acceptSnapshot(Snapshot snap) {
        this.snapshot = snap;
        this.entries.clear();
        this.offset = snap.index() + 1;
    }

    void stableTo(long index, long term) {
        if (entries.isEmpty()) {
            return;
        }

        if (index < offset) {
            return;
        }

        int arrayIdx = (int) (index - offset);
        if (arrayIdx >= entries.size()) {
            return;
        }

        if (entries.get(arrayIdx).term() == term) {
            entries.subList(0, arrayIdx + 1).clear();
            offset = index + 1;
        }
    }

    void snapshotStabilized() {
        snapshot = null;
    }

    LogEntry get(long index) {
        if (index < offset || index >= offset + entries.size()) {
            return null;
        }
        return entries.get((int) (index - offset));
    }

    Long term(long index) {
        if (snapshot != null && index == snapshot.index()) {
            return snapshot.term();
        }
        LogEntry entry = get(index);
        return entry != null ? entry.term() : null;
    }

    List<LogEntry> slice(long from, long to) {
        if (from >= offset + entries.size() || to <= offset || from >= to) {
            return List.of();
        }

        int start = Math.max(0, (int) (from - offset));
        int end = Math.min(entries.size(), (int) (to - offset));
        return List.copyOf(entries.subList(start, end));
    }

    List<LogEntry> entries() {
        return List.copyOf(entries);
    }

    Snapshot snapshot() {
        return snapshot;
    }

    long lastIndex() {
        if (!entries.isEmpty()) {
            return offset + entries.size() - 1;
        }
        if (snapshot != null) {
            return snapshot.index();
        }
        return 0;
    }

    long lastTerm() {
        if (!entries.isEmpty()) {
            return entries.getLast().term();
        }
        if (snapshot != null) {
            return snapshot.term();
        }
        return 0;
    }

    boolean hasEntries() {
        return !entries.isEmpty();
    }

    boolean hasSnapshot() {
        return snapshot != null;
    }

    long offset() {
        return offset;
    }
}
