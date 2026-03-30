package io.partdb.storage;

sealed interface ReserveResult {

    record Success(ReservationToken token) implements ReserveResult {}

    record Conflict() implements ReserveResult {}
}
