package io.partdb.client.testing;

import io.partdb.client.ServerEndpoint;

import java.io.IOException;

public interface TestClusterNode {
    String nodeId();

    ServerEndpoint endpoint();

    boolean isRunning();

    void start() throws IOException;

    void stop();

    default void assertStillRunning() {
    }
}
