package io.partdb.node;

import io.partdb.raft.runtime.RaftException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

final class FailureMapper {
    private FailureMapper() {
    }

    static Throwable map(Throwable error) {
        Throwable cause = (error instanceof CompletionException || error instanceof ExecutionException) && error.getCause() != null
            ? error.getCause()
            : error;
        return switch (cause) {
            case PartDbException _, IllegalArgumentException _ -> cause;
            case RaftException.NotLeader e -> new PartDbException.NotLeader(e.leader().map(io.partdb.raft.NodeId::value).orElse(null));
            case RaftException.Closed _ -> new PartDbException.NodeClosed();
            default -> new PartDbException.StorageFailure(
                cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName(),
                cause
            );
        };
    }
}
