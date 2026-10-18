package io.partdb.client;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.ErrorInfo;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.protobuf.StatusProto;

import java.net.ConnectException;
import java.util.Optional;

sealed interface Failure {

    String NOT_LEADER = "NOT_LEADER";
    String BUSY = "BUSY";
    String OUTCOME_UNKNOWN = "OUTCOME_UNKNOWN";
    String LEADER_ID = "leader_id";

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
        var info = errorInfo(error);
        if (info.isPresent()) {
            switch (info.get().getReason()) {
                case NOT_LEADER -> {
                    String leader = info.get().getMetadataOrDefault(LEADER_ID, "");
                    return new NotLeader(leader.isEmpty() ? Optional.empty() : Optional.of(leader), error);
                }
                case BUSY -> {
                    return new Busy(error);
                }
                case OUTCOME_UNKNOWN -> {
                    return new OutcomeUnknown(error);
                }
                default -> {
                }
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

    private static Optional<ErrorInfo> errorInfo(StatusRuntimeException error) {
        var status = StatusProto.fromThrowable(error);
        if (status == null) {
            return Optional.empty();
        }
        for (var detail : status.getDetailsList()) {
            if (detail.is(ErrorInfo.class)) {
                try {
                    return Optional.of(detail.unpack(ErrorInfo.class));
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
