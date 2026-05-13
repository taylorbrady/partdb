package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class InMemoryRaftLog implements RaftLogReader {
    private volatile RaftHardState hardState = RaftHardState.INITIAL;
    private final List<RaftLogEntry> entries = new ArrayList<>();
    private volatile RaftSnapshot snapshot;
    private volatile RaftMembership configuration;

    InMemoryRaftLog() {
        this(null);
    }

    InMemoryRaftLog(RaftMembership initialConfiguration) {
        this.configuration = initialConfiguration;
    }

    @Override
    public List<RaftLogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        List<RaftLogEntry> result = new ArrayList<>();
        long bytes = 0;
        for (RaftLogEntry entry : entries) {
            if (entry.index() >= fromIndex && entry.index() < toIndex) {
                result.add(entry);
                switch (entry) {
                    case RaftLogEntry.Data data -> bytes += data.data().size();
                    case RaftLogEntry.NoOp _, RaftLogEntry.Config _ -> {}
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
        for (RaftLogEntry entry : entries) {
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

    public void append(RaftHardState hardState, List<RaftLogEntry> newEntries) {
        if (hardState != null) {
            this.hardState = hardState;
        }

        if (newEntries.isEmpty()) {
            return;
        }

        long firstNewIndex = newEntries.getFirst().index();
        entries.removeIf(e -> e.index() >= firstNewIndex);
        entries.addAll(newEntries);

        for (RaftLogEntry entry : newEntries) {
            switch (entry) {
                case RaftLogEntry.Config config -> this.configuration = config.membership();
                case RaftLogEntry.Data _, RaftLogEntry.NoOp _ -> {}
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
        this.configuration = snapshot.membership();
        entries.removeIf(e -> e.index() <= snapshot.index());
    }

    public void compact(long index) {
        entries.removeIf(e -> e.index() <= index);
    }

    public void close() {
    }
}
