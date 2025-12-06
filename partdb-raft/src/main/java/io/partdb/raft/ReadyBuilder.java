package io.partdb.raft;

import java.util.ArrayList;
import java.util.List;

final class ReadyBuilder {
    private HardState hardState;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<Ready.Outbound> messages = new ArrayList<>();
    private final List<Ready.ApplyEntry> toApply = new ArrayList<>();
    private final List<Ready.ReadState> readStates = new ArrayList<>();
    private final List<Ready.MembershipChange> membershipChanges = new ArrayList<>();
    private Snapshot incomingSnapshot;
    private Ready.SnapshotToSend snapshotToSend;

    void setHardState(HardState hardState) {
        this.hardState = hardState;
    }

    void persist(LogEntry entry) {
        entries.add(entry);
    }

    void send(String to, RaftMessage message) {
        messages.add(new Ready.Outbound(to, message));
    }

    void apply(long index, long term, byte[] data) {
        toApply.add(new Ready.ApplyEntry(index, term, data));
    }

    void addReadState(long index, byte[] context) {
        readStates.add(new Ready.ReadState(index, context));
    }

    void addMembershipChange(long index, Membership previous, Membership current) {
        membershipChanges.add(new Ready.MembershipChange(index, previous, current));
    }

    void setIncomingSnapshot(Snapshot snapshot) {
        this.incomingSnapshot = snapshot;
    }

    void setSnapshotToSend(String peer, long index, long term) {
        this.snapshotToSend = new Ready.SnapshotToSend(peer, index, term);
    }

    Ready build() {
        boolean mustSync = hardState != null || !entries.isEmpty() || incomingSnapshot != null;

        var persist = new Ready.Persist(
            hardState,
            List.copyOf(entries),
            incomingSnapshot,
            mustSync
        );

        var apply = new Ready.Apply(
            List.copyOf(toApply),
            List.copyOf(readStates),
            List.copyOf(membershipChanges)
        );

        return new Ready(persist, List.copyOf(messages), apply, snapshotToSend);
    }
}
