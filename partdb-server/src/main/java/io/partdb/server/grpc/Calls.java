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
import io.partdb.node.NodeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class Calls {

    private static final Logger log = LoggerFactory.getLogger(Calls.class);
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
            case NodeException.NotLeader notLeader -> status(Code.UNAVAILABLE, notLeader.getMessage(), "NOT_LEADER",
                notLeader.leader().map(leader -> Map.of("leader_id", leader.value())).orElse(Map.of()));
            case NodeException.Busy busy -> status(Code.UNAVAILABLE, busy.getMessage(), "BUSY", Map.of());
            case NodeException.Unavailable unavailable -> status(Code.UNAVAILABLE, unavailable.getMessage(), null, Map.of());
            case NodeException.InvalidRequest invalid -> status(Code.INVALID_ARGUMENT, invalid.getMessage(), null, Map.of());
            case NodeException.Timeout timeout -> status(Code.DEADLINE_EXCEEDED, timeout.getMessage(), null, Map.of());
            case NodeException.OutcomeUnknown unknown -> status(Code.UNKNOWN, unknown.getMessage(), "OUTCOME_UNKNOWN", Map.of());
            case NodeException.Cancelled cancelled -> status(Code.CANCELLED, cancelled.getMessage(), null, Map.of());
            case NodeException.Internal internal -> internal(internal);
            default -> internal(error);
        };
    }

    static StatusRuntimeException invalid(String message) {
        return status(Code.INVALID_ARGUMENT, message, null, Map.of());
    }

    private static StatusRuntimeException internal(RuntimeException error) {
        log.atError().setCause(error).log("Request failed with an internal error");
        return status(Code.INTERNAL, "internal error", null, Map.of());
    }

    private static StatusRuntimeException status(Code code, String message, String reason, Map<String, String> metadata) {
        var status = Status.newBuilder().setCode(code.getNumber()).setMessage(message == null ? code.name() : message);
        if (reason != null) {
            status.addDetails(Any.pack(ErrorInfo.newBuilder().setReason(reason).setDomain(DOMAIN).putAllMetadata(metadata).build()));
        }
        return StatusProto.toStatusRuntimeException(status.build());
    }
}
