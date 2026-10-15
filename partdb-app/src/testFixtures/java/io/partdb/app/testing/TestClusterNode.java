package io.partdb.app.testing;

import io.partdb.client.ServerEndpoint;

import java.io.IOException;

public interface TestClusterNode {
    String nodeId();

    ServerEndpoint endpoint();

    boolean isStarted();

    void start() throws IOException;

    void stop();

    default void checkAlive() {
    }
}
