package io.partdb.raft;

import java.util.List;
import java.util.Objects;

public record Ready(
    Persist persist,
    List<Outbound> messages,
    Apply apply,
    SnapshotToSend snapshotToSend
) {
    public Ready {
        persist = Objects.requireNonNull(persist, "persist must not be null");
        messages = List.copyOf(Objects.requireNonNull(messages, "messages must not be null"));
        apply = Objects.requireNonNull(apply, "apply must not be null");
    }

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
        public Persist {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
        }

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
        public Apply {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            readStates = List.copyOf(Objects.requireNonNull(readStates, "readStates must not be null"));
            membershipChanges = List.copyOf(Objects.requireNonNull(membershipChanges, "membershipChanges must not be null"));
        }

        public static final Apply EMPTY = new Apply(List.of(), List.of(), List.of());

        public boolean hasWork() {
            return !entries.isEmpty() || !readStates.isEmpty() || !membershipChanges.isEmpty();
        }
    }

    public record Outbound(String to, RaftMessage message) {}

    public record ApplyEntry(long index, long term, byte[] data) {
        public ApplyEntry {
            Objects.requireNonNull(data, "data must not be null");
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    public record SnapshotToSend(String peer, long index, long term) {}

    public record ReadState(long index, byte[] context) {
        public ReadState {
            Objects.requireNonNull(context, "context must not be null");
            context = context.clone();
        }

        @Override
        public byte[] context() {
            return context.clone();
        }
    }

    public record MembershipChange(long index, Membership previous, Membership current) {}
}
