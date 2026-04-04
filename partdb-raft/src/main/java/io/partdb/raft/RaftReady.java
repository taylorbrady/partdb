package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public record RaftReady(
    Persistence persistence,
    List<Outbound> messages,
    Application application,
    Optional<SnapshotTransfer> snapshotTransfer
) {
    public RaftReady {
        persistence = Objects.requireNonNull(persistence, "persistence must not be null");
        messages = List.copyOf(Objects.requireNonNull(messages, "messages must not be null"));
        application = Objects.requireNonNull(application, "application must not be null");
        snapshotTransfer = Objects.requireNonNull(snapshotTransfer, "snapshotTransfer must not be null");
    }

    public static final RaftReady EMPTY = new RaftReady(
        Persistence.EMPTY,
        List.of(),
        Application.EMPTY,
        Optional.empty()
    );

    public boolean hasWork() {
        return persistence.hasWork() || !messages.isEmpty() || application.hasWork() || snapshotTransfer.isPresent();
    }

    public record Persistence(
        Optional<RaftPersistentState> persistentState,
        List<LogEntry> entries,
        Optional<RaftSnapshot> incomingSnapshot,
        boolean requiresSync
    ) {
        public Persistence {
            persistentState = Objects.requireNonNull(persistentState, "persistentState must not be null");
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            incomingSnapshot = Objects.requireNonNull(incomingSnapshot, "incomingSnapshot must not be null");
        }

        public static final Persistence EMPTY = new Persistence(Optional.empty(), List.of(), Optional.empty(), false);

        public boolean hasWork() {
            return persistentState.isPresent() || !entries.isEmpty() || incomingSnapshot.isPresent();
        }
    }

    public record Application(
        List<ApplyEntry> entries,
        List<ReadState> readStates,
        List<ConfigurationTransition> configurationTransitions,
        long appliedThroughIndex
    ) {
        public Application {
            entries = List.copyOf(Objects.requireNonNull(entries, "entries must not be null"));
            readStates = List.copyOf(Objects.requireNonNull(readStates, "readStates must not be null"));
            configurationTransitions = List.copyOf(Objects.requireNonNull(configurationTransitions, "configurationTransitions must not be null"));
            if (appliedThroughIndex < 0) {
                throw new IllegalArgumentException("appliedThroughIndex must not be negative");
            }
        }

        public static final Application EMPTY = new Application(List.of(), List.of(), List.of(), 0);

        public boolean hasWork() {
            return !entries.isEmpty() || !readStates.isEmpty() || !configurationTransitions.isEmpty() || appliedThroughIndex > 0;
        }
    }

    public record Outbound(String to, RaftMessage message) {}

    public record ApplyEntry(long index, long term, Bytes data) {
        public ApplyEntry {
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }

    public record SnapshotTransfer(String peer, long index, long term) {}

    public record ReadState(long index, Bytes context) {
        public ReadState {
            context = Objects.requireNonNull(context, "context must not be null");
        }
    }

    public record ConfigurationTransition(long index, RaftConfiguration previous, RaftConfiguration current) {}
}
