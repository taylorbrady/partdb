package io.partdb.node.runtime;

import io.partdb.consensus.ConsensusException;
import io.partdb.node.PartDbException;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

public final class NodeFailureMapper {
    private NodeFailureMapper() {
    }

    public static Throwable map(Throwable error) {
        Throwable cause = unwrap(error);
        return switch (cause) {
            case PartDbException _ -> cause;
            case ConsensusException.NotLeader e -> new PartDbException.NotLeader(e.leaderId().orElse(null));
            case ConsensusException.Shutdown _ -> new PartDbException.NodeClosed();
            case IllegalArgumentException _ -> cause;
            default -> new PartDbException.StorageFailure(
                cause.getMessage() != null ? cause.getMessage() : cause.getClass().getSimpleName(),
                cause
            );
        };
    }

    private static Throwable unwrap(Throwable error) {
        if (error instanceof CompletionException || error instanceof ExecutionException) {
            return error.getCause() != null ? error.getCause() : error;
        }
        return error;
    }
}
