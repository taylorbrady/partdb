package io.partdb.raft;

public record Progress(long nextIndex, long matchIndex) {

    public Progress() {
        this(1, 0);
    }

    public Progress decrementNext() {
        return new Progress(Math.max(1, nextIndex - 1), matchIndex);
    }

    public Progress withMatch(long matchIndex) {
        return new Progress(matchIndex + 1, matchIndex);
    }
}
