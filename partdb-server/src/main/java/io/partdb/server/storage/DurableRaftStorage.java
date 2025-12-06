package io.partdb.server.storage;

import io.partdb.raft.HardState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.Membership;
import io.partdb.raft.RaftStorage;
import io.partdb.raft.Snapshot;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DurableRaftStorage implements RaftStorage {

    private final WriteAheadLog wal;
    private final SnapshotStore snapshots;
    private final ReentrantReadWriteLock lock;

    private Snapshot currentSnapshot;
    private Membership membership;

    private DurableRaftStorage(WriteAheadLog wal, SnapshotStore snapshots,
                                Snapshot currentSnapshot, Membership membership) {
        this.wal = wal;
        this.snapshots = snapshots;
        this.lock = new ReentrantReadWriteLock();
        this.currentSnapshot = currentSnapshot;
        this.membership = membership;
    }

    public static DurableRaftStorage create(Path directory, Membership initialMembership) {
        Path walDir = directory.resolve("wal");
        Path snapDir = directory.resolve("snap");

        WriteAheadLog wal = WriteAheadLog.create(walDir);
        SnapshotStore snapshots = SnapshotStore.open(snapDir);

        return new DurableRaftStorage(wal, snapshots, null, initialMembership);
    }

    public static DurableRaftStorage open(Path directory) {
        Path walDir = directory.resolve("wal");
        Path snapDir = directory.resolve("snap");

        SnapshotStore snapshots = SnapshotStore.open(snapDir);
        Snapshot snapshot = snapshots.latest().orElse(null);

        WriteAheadLog wal = WriteAheadLog.open(walDir);

        Membership membership = null;
        if (snapshot != null) {
            membership = snapshot.membership();
        }

        membership = recoverMembershipFromLog(wal, membership);

        return new DurableRaftStorage(wal, snapshots, snapshot, membership);
    }

    private static Membership recoverMembershipFromLog(WriteAheadLog wal, Membership initial) {
        Membership membership = initial;
        long from = wal.firstIndex();
        long to = wal.lastIndex() + 1;

        List<LogEntry> entries = wal.entries(from, to, Long.MAX_VALUE);
        for (LogEntry entry : entries) {
            switch (entry) {
                case LogEntry.Config(long idx, long term, Membership m) -> membership = m;
                case LogEntry.Data _, LogEntry.NoOp _ -> {}
            }
        }

        return membership;
    }

    @Override
    public InitialState initialState() {
        lock.readLock().lock();
        try {
            return new InitialState(wal.hardState(), membership);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<LogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        lock.readLock().lock();
        try {
            return wal.entries(fromIndex, toIndex, maxBytes);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long term(long index) {
        lock.readLock().lock();
        try {
            if (currentSnapshot != null && index == currentSnapshot.index()) {
                return currentSnapshot.term();
            }
            return wal.term(index);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long firstIndex() {
        lock.readLock().lock();
        try {
            if (currentSnapshot != null) {
                return currentSnapshot.index() + 1;
            }
            return wal.firstIndex();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long lastIndex() {
        lock.readLock().lock();
        try {
            long walLast = wal.lastIndex();
            if (walLast > 0) {
                return walLast;
            }
            if (currentSnapshot != null) {
                return currentSnapshot.index();
            }
            return 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void append(HardState hardState, List<LogEntry> entries) {
        lock.writeLock().lock();
        try {
            wal.append(hardState, entries);

            for (LogEntry entry : entries) {
                switch (entry) {
                    case LogEntry.Config(long idx, long term, Membership m) -> membership = m;
                    case LogEntry.Data _, LogEntry.NoOp _ -> {}
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void sync() {
        lock.readLock().lock();
        try {
            wal.sync();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<Snapshot> snapshot() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(currentSnapshot);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void saveSnapshot(Snapshot snapshot) {
        lock.writeLock().lock();
        try {
            snapshots.save(snapshot);

            wal.recordSnapshotMarker(snapshot.index(), snapshot.term());
            wal.sync();

            wal.truncateBefore(snapshot.index());

            if (currentSnapshot != null) {
                snapshots.deleteOlderThan(snapshot.index());
            }

            currentSnapshot = snapshot;
            membership = snapshot.membership();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void compact(long index) {
        lock.writeLock().lock();
        try {
            wal.truncateBefore(index);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            wal.close();
            snapshots.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
