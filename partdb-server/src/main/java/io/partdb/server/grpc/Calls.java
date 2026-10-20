package io.partdb.server.grpc;

import com.google.protobuf.Any;
import com.google.rpc.Code;
import com.google.rpc.ErrorInfo;
import com.google.rpc.Status;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import io.partdb.grpc.error.v1.ErrorReason;
import io.partdb.grpc.error.v1.LeaderHint;
import io.partdb.node.NodeException;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class Calls {

    private static final Logger log = System.getLogger(Calls.class.getName());
    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
    private static final String DOMAIN = "partdb.io";

    private Calls() {
    }

    static <R> void unary(StreamObserver<R> responses, Function<Duration, R> call) {
        var thread = Thread.currentThread();
        Context.CancellationListener interrupt = _ -> thread.interrupt();
        var context = Context.current();
        context.addListener(interrupt, Runnable::run);
        try {
            R response = call.apply(timeout());
            responses.onNext(response);
            responses.onCompleted();
        } catch (RuntimeException e) {
            responses.onError(toStatus(e));
        } finally {
            context.removeListener(interrupt);
            Thread.interrupted();
        }
    }

    static Duration timeout() {
        Deadline deadline = Context.current().getDeadline();
        return deadline == null ? DEFAULT_TIMEOUT : Duration.ofNanos(Math.max(0, deadline.timeRemaining(TimeUnit.NANOSECONDS)));
    }

    static StatusRuntimeException toStatus(RuntimeException error) {
        return switch (error) {
            case StatusRuntimeException status -> status;
            case NodeException.NotLeader notLeader -> status(Code.UNAVAILABLE, notLeader.getMessage(), ErrorReason.NOT_LEADER,
                notLeader.leader().map(leader -> LeaderHint.newBuilder().setLeaderId(leader.value()).build()));
            case NodeException.Busy busy -> status(Code.UNAVAILABLE, busy.getMessage(), ErrorReason.BUSY, Optional.empty());
            case NodeException.Unavailable unavailable -> status(Code.UNAVAILABLE, unavailable.getMessage());
            case NodeException.InvalidRequest invalid -> status(Code.INVALID_ARGUMENT, invalid.getMessage());
            case NodeException.Timeout timeout -> status(Code.DEADLINE_EXCEEDED, timeout.getMessage());
            case NodeException.OutcomeUnknown unknown -> status(Code.UNKNOWN, unknown.getMessage(), ErrorReason.OUTCOME_UNKNOWN, Optional.empty());
            case NodeException.Cancelled cancelled -> status(Code.CANCELLED, cancelled.getMessage());
            case NodeException.Internal internal -> internal(internal);
            default -> internal(error);
        };
    }

    static StatusRuntimeException invalid(String message) {
        return status(Code.INVALID_ARGUMENT, message);
    }

    private static StatusRuntimeException internal(RuntimeException error) {
        log.log(Level.ERROR, "Request failed with an internal error", error);
        return status(Code.INTERNAL, "internal error");
    }

    private static StatusRuntimeException status(Code code, String message) {
        return StatusProto.toStatusRuntimeException(base(code, message).build());
    }

    private static StatusRuntimeException status(Code code, String message, ErrorReason reason, Optional<LeaderHint> hint) {
        var status = base(code, message).addDetails(Any.pack(ErrorInfo.newBuilder().setReason(reason.name()).setDomain(DOMAIN).build()));
        hint.ifPresent(leader -> status.addDetails(Any.pack(leader)));
        return StatusProto.toStatusRuntimeException(status.build());
    }

    private static Status.Builder base(Code code, String message) {
        return Status.newBuilder().setCode(code.getNumber()).setMessage(message == null ? code.name() : message);
    }
}
