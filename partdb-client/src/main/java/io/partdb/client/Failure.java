package io.partdb.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;
import io.partdb.grpc.error.v1.ErrorReason;
import io.partdb.grpc.error.v1.LeaderHint;

import java.net.ConnectException;
import java.util.Optional;

sealed interface Failure {

    StatusRuntimeException cause();

    record NotLeader(Optional<String> leader, StatusRuntimeException cause) implements Failure {}

    record Busy(StatusRuntimeException cause) implements Failure {}

    record Unreachable(boolean sent, StatusRuntimeException cause) implements Failure {}

    record DeadlineExceeded(StatusRuntimeException cause) implements Failure {}

    record Invalid(StatusRuntimeException cause) implements Failure {}

    record OutcomeUnknown(StatusRuntimeException cause) implements Failure {}

    record Cancelled(StatusRuntimeException cause) implements Failure {}

    record Internal(StatusRuntimeException cause) implements Failure {}

    static Failure of(StatusRuntimeException error) {
        var details = StatusProto.fromThrowable(error);
        var reason = detail(details, ErrorInfo.class).map(info -> reason(info.getReason())).orElse(ErrorReason.ERROR_REASON_UNSPECIFIED);
        switch (reason) {
            case NOT_LEADER -> {
                var leader = detail(details, LeaderHint.class).map(LeaderHint::getLeaderId).filter(id -> !id.isEmpty());
                return new NotLeader(leader, error);
            }
            case BUSY -> {
                return new Busy(error);
            }
            case OUTCOME_UNKNOWN -> {
                return new OutcomeUnknown(error);
            }
            case ERROR_REASON_UNSPECIFIED, UNRECOGNIZED -> {
            }
        }
        return switch (error.getStatus().getCode()) {
            case UNAVAILABLE -> new Unreachable(!refused(error), error);
            case DEADLINE_EXCEEDED -> new DeadlineExceeded(error);
            case INVALID_ARGUMENT, OUT_OF_RANGE, FAILED_PRECONDITION -> new Invalid(error);
            case CANCELLED -> new Cancelled(error);
            default -> new Internal(error);
        };
    }

    default String message() {
        Status status = cause().getStatus();
        return status.getDescription() != null ? status.getDescription() : status.getCode().name();
    }

    private static ErrorReason reason(String name) {
        try {
            return ErrorReason.valueOf(name);
        } catch (IllegalArgumentException _) {
            return ErrorReason.ERROR_REASON_UNSPECIFIED;
        }
    }

    private static <T extends Message> Optional<T> detail(com.google.rpc.Status status, Class<T> type) {
        if (status == null) {
            return Optional.empty();
        }
        for (var detail : status.getDetailsList()) {
            if (detail.is(type)) {
                try {
                    return Optional.of(detail.unpack(type));
                } catch (InvalidProtocolBufferException _) {
                    return Optional.empty();
                }
            }
        }
        return Optional.empty();
    }

    private static boolean refused(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause instanceof ConnectException) {
                return true;
            }
        }
        return false;
    }
}
