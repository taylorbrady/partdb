package io.partdb.raft.runtime;

import io.partdb.raft.RaftHardState;
import io.partdb.raft.RaftLogEntry;
import io.partdb.raft.RaftLogReader;
import io.partdb.raft.RaftMembership;
import io.partdb.raft.SnapshotMetadata;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class FileRaftStorage implements RaftLogReader, AutoCloseable {

    private final SegmentedRaftLog wal;
    private final SnapshotStore snapshots;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private SnapshotStore.SnapshotFile snapshot;
    private RaftMembership membership;

    private FileRaftStorage(SegmentedRaftLog wal, SnapshotStore snapshots, SnapshotStore.SnapshotFile snapshot) {
        this.wal = wal;
        this.snapshots = snapshots;
        this.snapshot = snapshot;
        this.membership = snapshot == null ? null : snapshot.metadata().membership();
        recoverMembershipFromLog();
    }

    static FileRaftStorage open(Path directory) {
        Path walDirectory = directory.resolve("wal");
        SnapshotStore snapshots = SnapshotStore.open(directory.resolve("snap"));
        SegmentedRaftLog wal = Files.exists(walDirectory)
            ? SegmentedRaftLog.open(walDirectory)
            : SegmentedRaftLog.create(walDirectory);
        return new FileRaftStorage(wal, snapshots, snapshots.latest().orElse(null));
    }

    private void recoverMembershipFromLog() {
        long committedThrough = Math.min(wal.hardState().commit(), wal.lastIndex());
        for (RaftLogEntry entry : wal.entries(wal.firstIndex(), committedThrough + 1, Long.MAX_VALUE)) {
            if (entry instanceof RaftLogEntry.Config(_, _, var committed)) {
                membership = committed;
            }
        }
    }

    RaftHardState hardState() {
        lock.readLock().lock();
        try {
            return wal.hardState();
        } finally {
            lock.readLock().unlock();
        }
    }

    Optional<RaftMembership> membership() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(membership);
        } finally {
            lock.readLock().unlock();
        }
    }

    Optional<SnapshotStore.SnapshotFile> snapshot() {
        lock.readLock().lock();
        try {
            return Optional.ofNullable(snapshot);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public List<RaftLogEntry> entries(long fromIndex, long toIndex, long maxBytes) {
        lock.readLock().lock();
        try {
            if (snapshot != null && fromIndex <= snapshot.metadata().index()) {
                throw new RaftException.Compacted(fromIndex, snapshot.metadata().index() + 1);
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
            if (snapshot != null && index == snapshot.metadata().index()) {
                return snapshot.metadata().term();
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
            return snapshot != null ? snapshot.metadata().index() + 1 : wal.firstIndex();
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
            return snapshot != null ? snapshot.metadata().index() : 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    void append(RaftHardState hardState, List<RaftLogEntry> entries) {
        lock.writeLock().lock();
        try {
            wal.append(hardState, entries);
            for (RaftLogEntry entry : entries) {
                if (entry instanceof RaftLogEntry.Config(_, _, var appended)) {
                    membership = appended;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    void sync() {
        lock.readLock().lock();
        try {
            wal.sync();
        } finally {
            lock.readLock().unlock();
        }
    }

    Path stagingFile() {
        return snapshots.stagingFile();
    }

    void installSnapshot(SnapshotMetadata metadata, Path stagedPayload) {
        lock.writeLock().lock();
        try {
            SnapshotStore.SnapshotFile installed = snapshots.install(metadata, stagedPayload);
            wal.recordSnapshotMarker(metadata.index(), metadata.term());
            wal.sync();
            wal.truncateBefore(metadata.index());
            snapshot = installed;
            membership = metadata.membership();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void close() {
        lock.writeLock().lock();
        try {
            wal.close();
        } finally {
            lock.writeLock().unlock();
        }
    }
}
