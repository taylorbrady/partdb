package io.partdb.transport.grpc;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbServerObservabilityTest {

    @TempDir
    Path tempDir;

    @Test
    void startRegistersNodeAndStorageMxBeans() throws Exception {
        int raftPort = freePort();
        int grpcPort = freePort();
        var config = PartDbServerConfig.create(
            "node1",
            Map.of(),
            tempDir.resolve("node1"),
            raftPort,
            grpcPort
        );

        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName nodeObjectName = new ObjectName("io.partdb:type=Node,nodeId=\"node1\"");
        ObjectName storageObjectName = new ObjectName("io.partdb:type=Storage,nodeId=\"node1\"");

        try (var server = new PartDbServer(config)) {
            server.start();

            assertTrue(mBeanServer.isRegistered(nodeObjectName));
            assertTrue(mBeanServer.isRegistered(storageObjectName));
            assertEquals("node1", mBeanServer.getAttribute(nodeObjectName, "NodeId"));
            assertEquals(Boolean.TRUE, mBeanServer.getAttribute(nodeObjectName, "Running"));
            awaitRole(mBeanServer, nodeObjectName, "LEADER");
            assertEquals(0, mBeanServer.getAttribute(storageObjectName, "SstableCount"));
        }

        assertFalse(mBeanServer.isRegistered(nodeObjectName));
        assertFalse(mBeanServer.isRegistered(storageObjectName));
    }

    private static void awaitRole(MBeanServer mBeanServer, ObjectName objectName, String expectedRole) throws Exception {
        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadlineNanos) {
            Object role = mBeanServer.getAttribute(objectName, "Role");
            if (expectedRole.equals(role)) {
                return;
            }
            Thread.sleep(Duration.ofMillis(10));
        }
        throw new AssertionError("Timed out waiting for role " + expectedRole);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
