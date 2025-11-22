package io.partdb.raft;

public record PeerReplicationState(
    long nextIndex,
    long matchIndex
) {
    public PeerReplicationState() {
        this(1, 0);
    }

    public PeerReplicationState advanceNext() {
        return new PeerReplicationState(nextIndex + 1, matchIndex);
    }

    public PeerReplicationState decrementNext() {
        return new PeerReplicationState(Math.max(1, nextIndex - 1), matchIndex);
    }

    public PeerReplicationState withMatch(long matchIndex) {
        return new PeerReplicationState(matchIndex + 1, matchIndex);
    }

    public boolean canReplicate(long lastLogIndex) {
        return nextIndex <= lastLogIndex;
    }
}
