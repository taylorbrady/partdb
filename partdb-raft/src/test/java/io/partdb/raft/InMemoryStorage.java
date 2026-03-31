package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class InMemoryStorage implements RaftLogView {
    private volatile RaftPersistentState hardState = RaftPersistentState.INITIAL;
    private final List<LogEntry> entries = new ArrayList<>();
    private volatile RaftSnapshot snapshot;
    private volatile RaftMembership membership;

    InMemoryStorage() {
        this(null);
    }

    InMemoryStorage(RaftMembership initialMembership) {
        this.membership = initialMembership;
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;
        for (LogEntry entry : entries) {
            if (entry.index() >= fromIndex && entry.index() < toIndex) {
                result.add(entry);
                switch (entry) {
                    case LogEntry.Data data -> bytes += data.data().size();
                    case LogEntry.NoOp _, LogEntry.Config _ -> {}
                }
                if (bytes >= maxBytes && !result.isEmpty()) {
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public long term(long index) {
        if (snapshot != null && index == snapshot.index()) {
            return snapshot.term();
        }
        for (LogEntry entry : entries) {
            if (entry.index() == index) {
                return entry.term();
            }
        }
        return 0;
    }

    @Override
    public long firstIndex() {
        if (entries.isEmpty()) {
            return snapshot != null ? snapshot.index() + 1 : 1;
        }
        return entries.getFirst().index();
    }

    @Override
    public long lastIndex() {
        if (entries.isEmpty()) {
            return snapshot != null ? snapshot.index() : 0;
        }
        return entries.getLast().index();
    }

    public void append(RaftPersistentState hardState, List<LogEntry> newEntries) {
        if (hardState != null) {
            this.hardState = hardState;
        }

        if (newEntries.isEmpty()) {
            return;
        }

        long firstNewIndex = newEntries.getFirst().index();
        entries.removeIf(e -> e.index() >= firstNewIndex);
        entries.addAll(newEntries);

        for (LogEntry entry : newEntries) {
            switch (entry) {
                case LogEntry.Config config -> this.membership = config.membership();
                case LogEntry.Data _, LogEntry.NoOp _ -> {}
            }
        }
    }

    public void sync() {
    }

    public Optional<RaftSnapshot> snapshot() {
        return Optional.ofNullable(snapshot);
    }

    public void saveSnapshot(RaftSnapshot snapshot) {
        this.snapshot = snapshot;
        this.membership = snapshot.membership();
        entries.removeIf(e -> e.index() <= snapshot.index());
    }

    public void compact(long index) {
        entries.removeIf(e -> e.index() <= index);
    }

    public void close() {
    }
}
