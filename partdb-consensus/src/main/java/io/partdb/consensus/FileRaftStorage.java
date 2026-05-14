package io.partdb.consensus;

import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.RaftSnapshot;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class FileRaftStorage implements RaftStorage {

    private final SegmentedRaftLog wal;
    private final FileSnapshotStore snapshots;
    private final ReentrantReadWriteLock lock;

    private RaftSnapshot currentSnapshot;
    private RaftMembership configuration;

    private FileRaftStorage(SegmentedRaftLog wal, FileSnapshotStore snapshots,
                             RaftSnapshot currentSnapshot, RaftMembership configuration) {
        this.wal = wal;
        this.snapshots = snapshots;
        this.lock = new ReentrantReadWriteLock();
        this.currentSnapshot = currentSnapshot;
        this.configuration = configuration;
    }

    public static FileRaftStorage create(Path directory, RaftMembership initialConfiguration) {
        Path walDir = directory.resolve("wal");
        Path snapDir = directory.resolve("snap");

        SegmentedRaftLog wal = SegmentedRaftLog.create(walDir);
        FileSnapshotStore snapshots = FileSnapshotStore.open(snapDir);

        return new FileRaftStorage(wal, snapshots, null, initialConfiguration);
    }

    public static FileRaftStorage open(Path directory) {
        Path walDir = directory.resolve("wal");
        Path snapDir = directory.resolve("snap");

        FileSnapshotStore snapshots = FileSnapshotStore.open(snapDir);
        RaftSnapshot snapshot = snapshots.latest().orElse(null);

        SegmentedRaftLog wal = SegmentedRaftLog.open(walDir);

        RaftMembership configuration = null;
        if (snapshot != null) {
            configuration = snapshot.membership();
        }

        configuration = recoverConfigurationFromLog(wal, configuration);

        return new FileRaftStorage(wal, snapshots, snapshot, configuration);
    }

    private static RaftMembership recoverConfigurationFromLog(SegmentedRaftLog wal, RaftMembership initial) {
        RaftMembership configuration = initial;
        long from = wal.firstIndex();
        long committedThrough = Math.min(wal.hardState().commit(), wal.lastIndex());
        long to = committedThrough + 1;

        List<RaftLogEntry> entries = wal.entries(from, to, Long.MAX_VALUE);
        for (RaftLogEntry entry : entries) {
            switch (entry) {
                case RaftLogEntry.Config(long idx, long term, RaftMembership c) -> configuration = c;
                case RaftLogEntry.Data _, RaftLogEntry.NoOp _ -> {}
            }
        }

        return configuration;
    }

    @Override
    public RaftStorage.Bootstrap bootstrap() {
        lock.readLock().lock();
        try {
            return new RaftStorage.Bootstrap(Optional.of(wal.hardState()), Optional.ofNullable(configuration));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<RaftLogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        lock.readLock().lock();
        try {
            if (currentSnapshot != null && fromIndex <= currentSnapshot.index()) {
                throw new ConsensusException.Compaction(fromIndex, currentSnapshot.index() + 1);
            }
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
    public void append(RaftHardState hardState, List<RaftLogEntry> entries) {
        lock.writeLock().lock();
        try {
            wal.append(hardState, entries);

            for (RaftLogEntry entry : entries) {
                switch (entry) {
                    case RaftLogEntry.Config(long idx, long term, RaftMembership c) -> configuration = c;
                    case RaftLogEntry.Data _, RaftLogEntry.NoOp _ -> {}
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
            configuration = snapshot.membership();
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
