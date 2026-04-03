package io.partdb.transport.grpc;

import io.partdb.node.PartDbNode;
import io.partdb.node.PartDbNodeMXBean;
import io.partdb.node.PartDbStorageMXBean;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;

final class JmxRegistrations implements AutoCloseable {
    private final MBeanServer mBeanServer;
    private final PartDbNode node;
    private final ObjectName nodeObjectName;
    private final ObjectName storageObjectName;
    private boolean registered;

    JmxRegistrations(PartDbNode node) {
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        this.node = node;
        String nodeId = node.cluster().status().nodeId();
        this.nodeObjectName = objectName("Node", nodeId);
        this.storageObjectName = objectName("Storage", nodeId);
        this.registered = false;
    }

    void register() {
        if (registered) {
            return;
        }

        try {
            mBeanServer.registerMBean(
                new StandardMBean(new NodeMBean(node), PartDbNodeMXBean.class, true),
                nodeObjectName
            );
            mBeanServer.registerMBean(
                new StandardMBean(new StorageMBean(node), PartDbStorageMXBean.class, true),
                storageObjectName
            );
            registered = true;
        } catch (Exception e) {
            close();
            throw new IllegalStateException("Failed to register JMX observability beans", e);
        }
    }

    @Override
    public void close() {
        unregister(storageObjectName);
        unregister(nodeObjectName);
        registered = false;
    }

    private void unregister(ObjectName objectName) {
        try {
            if (mBeanServer.isRegistered(objectName)) {
                mBeanServer.unregisterMBean(objectName);
            }
        } catch (InstanceNotFoundException | MBeanRegistrationException ignored) {
        }
    }

    private static ObjectName objectName(String type, String nodeId) {
        try {
            return new ObjectName("io.partdb:type=%s,nodeId=%s".formatted(type, ObjectName.quote(nodeId)));
        } catch (MalformedObjectNameException e) {
            throw new IllegalArgumentException("Invalid JMX object name", e);
        }
    }

    private record NodeMBean(PartDbNode node) implements PartDbNodeMXBean {
        @Override
        public String getNodeId() {
            return node.cluster().status().nodeId();
        }

        @Override
        public boolean isRunning() {
            return node.cluster().status().running();
        }

        @Override
        public String getRole() {
            return node.cluster().status().role().name();
        }

        @Override
        public String getLeaderId() {
            return node.cluster().status().leaderId().orElse("");
        }

        @Override
        public long getCurrentTerm() {
            return node.cluster().status().term();
        }

        @Override
        public long getCommitIndex() {
            return node.cluster().status().commitIndex();
        }

        @Override
        public long getAppliedIndex() {
            return node.cluster().status().appliedIndex();
        }

        @Override
        public long getLastLeaderChangeEpochMillis() {
            return node.cluster().status().lastLeaderChangeTime()
                .map(java.time.Instant::toEpochMilli)
                .orElse(0L);
        }

        @Override
        public long getProposalCount() {
            return node.maintenance().nodeMetrics().proposalCount();
        }

        @Override
        public long getProposalFailureCount() {
            return node.maintenance().nodeMetrics().proposalFailureCount();
        }
    }

    private record StorageMBean(PartDbNode node) implements PartDbStorageMXBean {
        @Override
        public long getActiveMemtableBytes() {
            return node.maintenance().storageMetrics().activeMemtableBytes();
        }

        @Override
        public int getImmutableMemtableCount() {
            return node.maintenance().storageMetrics().immutableMemtableCount();
        }

        @Override
        public int getSstableCount() {
            return node.maintenance().storageMetrics().sstableCount();
        }

        @Override
        public long getTotalSstableBytes() {
            return node.maintenance().storageMetrics().totalSstableBytes();
        }

        @Override
        public int getActiveCompactions() {
            return node.maintenance().storageMetrics().activeCompactions();
        }

        @Override
        public long getCompletedCompactions() {
            return node.maintenance().storageMetrics().completedCompactions();
        }

        @Override
        public long getFailedCompactions() {
            return node.maintenance().storageMetrics().failedCompactions();
        }

        @Override
        public long getLastCompactionDurationMillis() {
            return node.maintenance().storageMetrics().lastCompactionDuration().toMillis();
        }

        @Override
        public long getCheckpointCount() {
            return node.maintenance().storageMetrics().checkpointCount();
        }

        @Override
        public long getRestoreCount() {
            return node.maintenance().storageMetrics().restoreCount();
        }

        @Override
        public long getLastCheckpointDurationMillis() {
            return node.maintenance().storageMetrics().lastCheckpointDuration().toMillis();
        }

        @Override
        public long getLastRestoreDurationMillis() {
            return node.maintenance().storageMetrics().lastRestoreDuration().toMillis();
        }
    }
}
