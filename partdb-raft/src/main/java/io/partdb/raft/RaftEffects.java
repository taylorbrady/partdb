package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record RaftEffects(
    Persistence persistence,
    List<Outbound> messages,
    Application application,
    Optional<SnapshotNeeded> snapshotNeeded
) {
    public RaftEffects {
        persistence = Objects.requireNonNull(persistence, "persistence must not be null");
        messages = List.copyOf(Objects.requireNonNull(messages, "messages must not be null"));
        application = Objects.requireNonNull(application, "application must not be null");
        snapshotNeeded = Objects.requireNonNull(snapshotNeeded, "snapshotNeeded must not be null");
    }

    public boolean hasWork() {
        return persistence.hasWork() || !messages.isEmpty() || application.hasWork() || snapshotNeeded.isPresent();
    }

    public record Persistence(
        Optional<RaftHardState> hardState,
        List<RaftLogEntry> entries,
        Optional<SnapshotMetadata> incomingSnapshot
    ) {
        public Persistence {
            hardState = Objects.requireNonNull(hardState, "hardState must not be null");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            incomingSnapshot = Objects.requireNonNull(incomingSnapshot, "incomingSnapshot must not be null");
        }

        public boolean hasWork() {
            return hardState.isPresent() || !entries.isEmpty() || incomingSnapshot.isPresent();
        }
    }

    public record Application(
        List<ApplyEntry> entries,
        List<ReadState> readStates,
        List<MembershipTransition> membershipTransitions,
        long appliedThroughIndex
    ) {
        public Application {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            readStates = List.copyOf(Objects.requireNonNull(readStates, "readStates must not be null"));
            membershipTransitions = List.copyOf(Objects.requireNonNull(membershipTransitions, "membershipTransitions must not be null"));
            if (appliedThroughIndex < 0) {
                throw new IllegalArgumentException("appliedThroughIndex must not be negative");
            }
        }

        public boolean hasWork() {
            return !entries.isEmpty() || !readStates.isEmpty() || !membershipTransitions.isEmpty() || appliedThroughIndex > 0;
        }
    }

    public record Outbound(String to, RaftMessage message) {}

    public record ApplyEntry(long index, long term, Bytes data) {
        public ApplyEntry {
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }

    public record SnapshotNeeded(String peer, long index, long term) {}

    public record ReadState(long index, long context) {}

    public record MembershipTransition(long index, RaftMembership previous, RaftMembership current) {}
}
