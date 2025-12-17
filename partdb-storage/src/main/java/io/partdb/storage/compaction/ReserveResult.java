package io.partdb.storage.compaction;

public sealed interface ReserveResult {

    record Success(ReservationToken token) implements ReserveResult {}

    record Conflict() implements ReserveResult {}
}
