package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class RaftStepAccumulator {
    private RaftPersistentState persistentState;
    private final List<LogEntry> entries = new ArrayList<>();
    private final List<RaftReady.Outbound> messages = new ArrayList<>();
    private final List<RaftReady.ApplyEntry> toApply = new ArrayList<>();
    private final List<RaftReady.ReadState> readStates = new ArrayList<>();
    private final List<RaftReady.MembershipTransition> membershipTransitions = new ArrayList<>();
    private long appliedThroughIndex;
    private RaftSnapshot incomingSnapshot;
    private RaftReady.SnapshotTransfer snapshotTransfer;

    void setPersistentState(RaftPersistentState persistentState) {
        this.persistentState = persistentState;
    }

    void persist(LogEntry entry) {
        entries.add(entry);
    }

    void send(String to, RaftMessage message) {
        messages.add(new RaftReady.Outbound(to, message));
    }

    void apply(long index, long term, Bytes data) {
        toApply.add(new RaftReady.ApplyEntry(index, term, data));
    }

    void addReadState(long index, Bytes context) {
        readStates.add(new RaftReady.ReadState(index, context));
    }

    void addMembershipTransition(long index, RaftMembership previous, RaftMembership current) {
        membershipTransitions.add(new RaftReady.MembershipTransition(index, previous, current));
    }

    void advanceAppliedThrough(long index) {
        appliedThroughIndex = Math.max(appliedThroughIndex, index);
    }

    void setIncomingSnapshot(RaftSnapshot snapshot) {
        this.incomingSnapshot = snapshot;
    }

    void setSnapshotTransfer(String peer, long index, long term) {
        this.snapshotTransfer = new RaftReady.SnapshotTransfer(peer, index, term);
    }

    RaftReady finish() {
        boolean requiresSync = persistentState != null || !entries.isEmpty() || incomingSnapshot != null;

        var persistence = new RaftReady.Persistence(
            Optional.ofNullable(persistentState),
            List.copyOf(entries),
            Optional.ofNullable(incomingSnapshot),
            requiresSync
        );

        var application = new RaftReady.Application(
            List.copyOf(toApply),
            List.copyOf(readStates),
            List.copyOf(membershipTransitions),
            appliedThroughIndex
        );

        return new RaftReady(persistence, List.copyOf(messages), application, Optional.ofNullable(snapshotTransfer));
    }
}
