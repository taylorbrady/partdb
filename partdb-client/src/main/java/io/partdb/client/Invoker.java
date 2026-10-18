package io.partdb.client;

import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

final class Invoker {

    enum Kind { READ, WRITE }

    private final Connections connections;
    private final Duration timeout;
    private final RetryPolicy retry;

    Invoker(Connections connections, Duration timeout, RetryPolicy retry) {
        this.connections = connections;
        this.timeout = timeout;
        this.retry = retry;
    }

    <T> T call(Kind kind, BiFunction<ManagedChannel, Deadline, T> call) {
        var deadline = deadline();
        Failure last = null;
        for (int attempt = 0; ; attempt++) {
            if (deadline.isExpired()) {
                throw exhausted(kind, last);
            }
            var endpoint = connections.preferred();
            try {
                return call.apply(connections.channel(endpoint), deadline);
            } catch (StatusRuntimeException error) {
                last = Failure.of(error);
            }
            switch (last) {
                case Failure.NotLeader(var leader, _) -> connections.redirect(endpoint, leader, deadline);
                case Failure.Busy _ -> {}
                case Failure.Unreachable(boolean sent, _) when kind == Kind.READ || !sent -> connections.rotate(endpoint);
                default -> throw exception(kind, last, endpoint);
            }
            boolean moved = !connections.preferred().equals(endpoint);
            if (!moved || attempt >= connections.endpoints().size()) {
                pause(retry.backoff(attempt), deadline);
            }
        }
    }

    <T> T read(ServerEndpoint endpoint, BiFunction<ManagedChannel, Deadline, T> call) {
        try {
            return call.apply(connections.channel(endpoint), deadline());
        } catch (StatusRuntimeException error) {
            throw exception(Kind.READ, Failure.of(error), endpoint);
        }
    }

    private Deadline deadline() {
        return Deadline.after(timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private static PartDbException exception(Kind kind, Failure failure, ServerEndpoint endpoint) {
        return switch (failure) {
            case Failure.Unreachable(boolean sent, var cause) when kind == Kind.WRITE && sent ->
                new PartDbException.OutcomeUnknown("the connection to " + endpoint + " failed during a write", cause);
            case Failure.NotLeader _, Failure.Busy _, Failure.Unreachable _ -> new PartDbException.Unavailable(failure.message(), failure.cause());
            case Failure.DeadlineExceeded exceeded when kind == Kind.WRITE ->
                new PartDbException.OutcomeUnknown("the write did not finish before its deadline", exceeded.cause());
            case Failure.DeadlineExceeded exceeded -> new PartDbException.Timeout("the read did not finish before its deadline", exceeded.cause());
            case Failure.Invalid invalid -> new PartDbException.InvalidRequest(invalid.message(), invalid.cause());
            case Failure.OutcomeUnknown unknown -> new PartDbException.OutcomeUnknown(unknown.message(), unknown.cause());
            case Failure.Cancelled cancelled -> new PartDbException.Cancelled(cancelled.message(), cancelled.cause());
            case Failure.Internal internal -> new PartDbException.Internal(internal.message(), internal.cause());
        };
    }

    private static PartDbException exhausted(Kind kind, Failure last) {
        var cause = last == null ? null : last.cause();
        String message = last == null ? "the request deadline expired" : "no node accepted the request before its deadline: " + last.message();
        return switch (last) {
            case Failure.Unreachable(boolean sent, _) when kind == Kind.WRITE && sent -> new PartDbException.OutcomeUnknown(message, cause);
            case null -> new PartDbException.Timeout(message, null);
            default -> new PartDbException.Unavailable(message, cause);
        };
    }

    private static void pause(Duration backoff, Deadline deadline) {
        long nanos = Math.min(backoff.toNanos(), deadline.timeRemaining(TimeUnit.NANOSECONDS));
        if (nanos <= 0) {
            return;
        }
        try {
            Thread.sleep(Duration.ofNanos(nanos));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PartDbException.Cancelled("interrupted while retrying", null);
        }
    }
}
