package io.partdb.raft;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

public record RaftEffects(
    Optional<RaftHardState> hardState,
    Optional<SnapshotMetadata> snapshot,
    List<RaftLogEntry> entries,
    List<Envelope> messages,
    List<RaftLogEntry> committed,
    List<ProposalResult> proposals,
    List<ReadResult> reads,
    Set<String> snapshotRequests
) {
    public RaftEffects {
        Objects.requireNonNull(hardState, "hardState must not be null");
        Objects.requireNonNull(snapshot, "snapshot must not be null");
        entries = List.copyOf(entries);
        messages = List.copyOf(messages);
        committed = List.copyOf(committed);
        proposals = List.copyOf(proposals);
        reads = List.copyOf(reads);
        snapshotRequests = Collections.unmodifiableSortedSet(new TreeSet<>(snapshotRequests));
    }

    public boolean isEmpty() {
        return hardState.isEmpty()
            && snapshot.isEmpty()
            && entries.isEmpty()
            && messages.isEmpty()
            && committed.isEmpty()
            && proposals.isEmpty()
            && reads.isEmpty()
            && snapshotRequests.isEmpty();
    }

    public Optional<RaftInput.Persisted> persisted() {
        if (!entries.isEmpty()) {
            var last = entries.getLast();
            return Optional.of(new RaftInput.Persisted(last.index(), last.term()));
        }
        return snapshot.map(metadata -> new RaftInput.Persisted(metadata.index(), metadata.term()));
    }

    public sealed interface ProposalResult {
        long id();

        record Accepted(long id, long index, long term) implements ProposalResult {}

        record Rejected(long id) implements ProposalResult {}
    }

    public sealed interface ReadResult {
        long id();

        record Confirmed(long id, long index) implements ReadResult {}

        record Rejected(long id) implements ReadResult {}
    }
}
