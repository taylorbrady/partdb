package io.partdb.app;

import io.partdb.client.ClusterClientConfig;
import io.partdb.client.KvClientConfig;
import io.partdb.client.ServerEndpoint;

import java.io.PrintStream;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

record CliRuntime(PrintStream out, PrintStream err) {
    static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    CliRuntime {
        out = Objects.requireNonNull(out, "out must not be null");
        err = Objects.requireNonNull(err, "err must not be null");
    }

    ClusterClientConfig clusterClientConfig(ServerEndpoint endpoint) {
        return ClusterClientConfig.defaultConfig(endpoint);
    }

    KvClientConfig kvClientConfig(ServerEndpoint endpoint) {
        return KvClientConfig.defaultConfig(endpoint);
    }

    <T> T await(CompletableFuture<T> future) throws TimeoutException, ExecutionException, InterruptedException {
        return future.get(REQUEST_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    int timeout() {
        return error("request timed out after " + REQUEST_TIMEOUT.toSeconds() + " seconds");
    }

    int error(String message) {
        err.println("Error: " + message);
        return 1;
    }

    static String rootCauseMessage(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message != null ? message : cause.getClass().getSimpleName();
    }
}
