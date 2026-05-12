package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

final class RaftEffectAccumulator {
    private RaftHardState hardState;
    private final List<RaftLogEntry> entries = new ArrayList<>();
    private final List<RaftEffects.Outbound> messages = new ArrayList<>();
    private final List<RaftEffects.ApplyEntry> toApply = new ArrayList<>();
    private final List<RaftEffects.ReadState> readStates = new ArrayList<>();
    private final List<RaftEffects.MembershipTransition> membershipTransitions = new ArrayList<>();
    private long appliedThroughIndex;
    private RaftSnapshot incomingSnapshot;
    private RaftEffects.SnapshotTransfer snapshotTransfer;

    void setHardState(RaftHardState hardState) {
        this.hardState = hardState;
    }

    void persist(RaftLogEntry entry) {
        entries.add(entry);
    }

    void send(String to, RaftMessage message) {
        messages.add(new RaftEffects.Outbound(to, message));
    }

    void apply(long index, long term, Bytes data) {
        toApply.add(new RaftEffects.ApplyEntry(index, term, data));
    }

    void addReadState(long index, Bytes context) {
        readStates.add(new RaftEffects.ReadState(index, context));
    }

    void addMembershipTransition(long index, RaftMembership previous, RaftMembership current) {
        membershipTransitions.add(new RaftEffects.MembershipTransition(index, previous, current));
    }

    void advanceAppliedThrough(long index) {
        appliedThroughIndex = Math.max(appliedThroughIndex, index);
    }

    void setIncomingSnapshot(RaftSnapshot snapshot) {
        this.incomingSnapshot = snapshot;
    }

    void setSnapshotTransfer(String peer, long index, long term) {
        this.snapshotTransfer = new RaftEffects.SnapshotTransfer(peer, index, term);
    }

    RaftEffects finish() {
        boolean requiresSync = hardState != null || !entries.isEmpty() || incomingSnapshot != null;

        var persistence = new RaftEffects.Persistence(
            Optional.ofNullable(hardState),
            List.copyOf(entries),
            Optional.ofNullable(incomingSnapshot),
            requiresSync
        );

        var application = new RaftEffects.Application(
            List.copyOf(toApply),
            List.copyOf(readStates),
            List.copyOf(membershipTransitions),
            appliedThroughIndex
        );

        return new RaftEffects(persistence, List.copyOf(messages), application, Optional.ofNullable(snapshotTransfer));
    }
}
