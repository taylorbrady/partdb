package io.partdb.raft;

import java.util.List;

public record Ready(
    Persist persist,
    List<Outbound> messages,
    Apply apply,
    SnapshotToSend snapshotToSend
) {
    public static final Ready EMPTY = new Ready(Persist.EMPTY, List.of(), Apply.EMPTY, null);

    public boolean hasWork() {
        return persist.hasWork() || !messages.isEmpty() || apply.hasWork() || snapshotToSend != null;
    }

    public record Persist(
        HardState hardState,
        List<LogEntry> entries,
        Snapshot incomingSnapshot,
        boolean mustSync
    ) {
        public static final Persist EMPTY = new Persist(null, List.of(), null, false);

        public boolean hasWork() {
            return hardState != null || !entries.isEmpty() || incomingSnapshot != null;
        }
    }

    public record Apply(
        List<ApplyEntry> entries,
        List<ReadState> readStates,
        List<MembershipChange> membershipChanges
    ) {
        public static final Apply EMPTY = new Apply(List.of(), List.of(), List.of());

        public boolean hasWork() {
            return !entries.isEmpty() || !readStates.isEmpty() || !membershipChanges.isEmpty();
        }
    }

    public record Outbound(String to, RaftMessage message) {}

    public record ApplyEntry(long index, long term, byte[] data) {}

    public record SnapshotToSend(String peer, long index, long term) {}

    public record ReadState(long index, byte[] context) {}

    public record MembershipChange(long index, Membership previous, Membership current) {}
}
