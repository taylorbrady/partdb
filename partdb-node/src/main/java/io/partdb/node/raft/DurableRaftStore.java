package io.partdb.node.raft;

import io.partdb.raft.RaftPersistentState;
import io.partdb.raft.LogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftSnapshot;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class DurableRaftStore implements RaftStore {

    private final WriteAheadLog wal;
    private final SnapshotStore snapshots;
    private final ReentrantReadWriteLock lock;

    private RaftSnapshot currentSnapshot;
    private RaftMembership membership;

    private DurableRaftStore(WriteAheadLog wal, SnapshotStore snapshots,
                             RaftSnapshot currentSnapshot, RaftMembership membership) {
        this.wal = wal;
        this.snapshots = snapshots;
        this.lock = new ReentrantReadWriteLock();
        this.currentSnapshot = currentSnapshot;
        this.membership = membership;
    }

    public static DurableRaftStore create(Path directory, RaftMembership initialMembership) {
        Path walDir = directory.resolve("wal");
        Path snapDir = directory.resolve("snap");

        WriteAheadLog wal = WriteAheadLog.create(walDir);
        SnapshotStore snapshots = SnapshotStore.open(snapDir);

        return new DurableRaftStore(wal, snapshots, null, initialMembership);
    }

    public static DurableRaftStore open(Path directory) {
        Path walDir = directory.resolve("wal");
        Path snapDir = directory.resolve("snap");

        SnapshotStore snapshots = SnapshotStore.open(snapDir);
        RaftSnapshot snapshot = snapshots.latest().orElse(null);

        WriteAheadLog wal = WriteAheadLog.open(walDir);

        RaftMembership membership = null;
        if (snapshot != null) {
            membership = snapshot.membership();
        }

        membership = recoverMembershipFromLog(wal, membership);

        return new DurableRaftStore(wal, snapshots, snapshot, membership);
    }

    private static RaftMembership recoverMembershipFromLog(WriteAheadLog wal, RaftMembership initial) {
        RaftMembership membership = initial;
        long from = wal.firstIndex();
        long to = wal.lastIndex() + 1;

        List<LogEntry> entries = wal.entries(from, to, Long.MAX_VALUE);
        for (LogEntry entry : entries) {
            switch (entry) {
                case LogEntry.Config(long idx, long term, RaftMembership m) -> membership = m;
                case LogEntry.Data _, LogEntry.NoOp _ -> {}
            }
        }

        return membership;
    }

    @Override
    public RaftStore.Bootstrap bootstrap() {
        lock.readLock().lock();
        try {
            return new RaftStore.Bootstrap(Optional.of(wal.hardState()), Optional.ofNullable(membership));
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
    public void append(RaftPersistentState hardState, List<LogEntry> entries) {
        lock.writeLock().lock();
        try {
            wal.append(hardState, entries);

            for (LogEntry entry : entries) {
                switch (entry) {
                    case LogEntry.Config(long idx, long term, RaftMembership m) -> membership = m;
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
    public Optional<RaftSnapshot> snapshot() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(currentSnapshot);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void saveSnapshot(RaftSnapshot snapshot) {
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
