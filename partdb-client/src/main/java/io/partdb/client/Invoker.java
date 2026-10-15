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
        var deadline = Deadline.after(timeout.toNanos(), TimeUnit.NANOSECONDS);
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
                case Failure.Unreachable unreachable ->
                    throw new PartDbClientException.OutcomeUnknown("the connection to " + endpoint + " failed during a write", unreachable.cause());
                case Failure.DeadlineExceeded exceeded when kind == Kind.WRITE ->
                    throw new PartDbClientException.OutcomeUnknown("the write did not finish before its deadline", exceeded.cause());
                case Failure.DeadlineExceeded exceeded ->
                    throw new PartDbClientException.Timeout("the read did not finish before its deadline", exceeded.cause());
                case Failure.Invalid invalid -> throw new PartDbClientException.InvalidRequest(invalid.message(), invalid.cause());
                case Failure.OutcomeUnknown unknown -> throw new PartDbClientException.OutcomeUnknown(unknown.message(), unknown.cause());
                case Failure.Cancelled cancelled -> throw new PartDbClientException.Cancelled(cancelled.message(), cancelled.cause());
                case Failure.Internal internal -> throw new PartDbClientException.Internal(internal.message(), internal.cause());
            }
            boolean moved = !connections.preferred().equals(endpoint);
            if (!moved || attempt >= connections.endpoints().size()) {
                pause(retry.backoff(attempt), deadline);
            }
        }
    }

    private static PartDbClientException exhausted(Kind kind, Failure last) {
        var cause = last == null ? null : last.cause();
        String message = last == null ? "the request deadline expired" : "no node accepted the request before its deadline: " + last.message();
        return switch (last) {
            case Failure.Unreachable(boolean sent, _) when kind == Kind.WRITE && sent -> new PartDbClientException.OutcomeUnknown(message, cause);
            case null -> new PartDbClientException.Timeout(message, null);
            default -> new PartDbClientException.Unavailable(message, cause);
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
            throw new PartDbClientException.Cancelled("interrupted while retrying", null);
        }
    }
}
