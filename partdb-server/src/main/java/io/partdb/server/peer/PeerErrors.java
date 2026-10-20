package io.partdb.server.peer;

import io.grpc.Status;

final class PeerErrors {

    private PeerErrors() {
    }

    static String describe(Throwable error) {
        var status = Status.fromThrowable(error);
        return status.getDescription() == null ? status.getCode().name() : status.getCode() + ": " + status.getDescription();
    }
}
