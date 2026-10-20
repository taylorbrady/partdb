package io.partdb.raft;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public record Ready(
    Optional<HardState> hardState,
    Optional<SnapshotMetadata> snapshot,
    List<LogEntry> entries,
    List<Envelope> messages,
    List<Envelope> persistedMessages,
    List<LogEntry> committed,
    List<Outcome> outcomes,
    Set<NodeId> snapshotRequests
) {
    public Ready {
        Objects.requireNonNull(hardState, "hardState must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        entries = List.copyOf(entries);
        messages = List.copyOf(messages);
        persistedMessages = List.copyOf(persistedMessages);
        committed = List.copyOf(committed);
        outcomes = List.copyOf(outcomes);
        snapshotRequests = Collections.unmodifiableSortedSet(new TreeSet<>(snapshotRequests));
    }

    public boolean isEmpty() {
        return hardState.isEmpty()
            && snapshot.isEmpty()
            && entries.isEmpty()
            && messages.isEmpty()
            && persistedMessages.isEmpty()
            && committed.isEmpty()
            && outcomes.isEmpty()
            && snapshotRequests.isEmpty();
    }

    public Optional<Input.Persisted> persisted() {
        if (!entries.isEmpty()) {
            var last = entries.getLast();
            return Optional.of(new Input.Persisted(last.index(), last.term()));
        }
        return snapshot.map(metadata -> new Input.Persisted(metadata.index(), metadata.term()));
    }
}
