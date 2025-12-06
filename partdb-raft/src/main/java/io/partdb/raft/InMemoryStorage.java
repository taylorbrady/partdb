package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class InMemoryStorage implements RaftStorage {
    private volatile HardState hardState = HardState.INITIAL;
    private final List<LogEntry> entries = new ArrayList<>();
    private volatile Snapshot snapshot;
    private volatile Membership membership;

    public InMemoryStorage() {
        this(null);
    }

    public InMemoryStorage(Membership initialMembership) {
        this.membership = initialMembership;
    }

    @Override
    public InitialState initialState() {
        return new InitialState(hardState, membership);
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        List<LogEntry> result = new ArrayList<>();
        long bytes = 0;
        for (LogEntry entry : entries) {
            if (entry.index() >= fromIndex && entry.index() < toIndex) {
                result.add(entry);
                switch (entry) {
                    case LogEntry.Data data -> bytes += data.data().length;
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

    @Override
    public void append(HardState hardState, List<LogEntry> newEntries) {
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

    @Override
    public void sync() {
    }

    @Override
    public Optional<Snapshot> snapshot() {
        return Optional.ofNullable(snapshot);
    }

    @Override
    public void saveSnapshot(Snapshot snapshot) {
        this.snapshot = snapshot;
        this.membership = snapshot.membership();
        entries.removeIf(e -> e.index() <= snapshot.index());
    }

    @Override
    public void compact(long index) {
        entries.removeIf(e -> e.index() <= index);
    }

    @Override
    public void close() {
    }
}
