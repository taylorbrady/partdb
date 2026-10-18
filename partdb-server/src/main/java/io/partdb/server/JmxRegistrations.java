package io.partdb.server;

import io.partdb.node.Node;
import io.partdb.raft.NodeId;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.StandardMBean;

final class JmxRegistrations implements AutoCloseable {
    private final MBeanServer mBeanServer;
    private final Node node;
    private final ObjectName nodeObjectName;
    private final ObjectName storageObjectName;
    private boolean registered;

    JmxRegistrations(Node node) {
        this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
        this.node = node;
        String nodeId = node.cluster().status().nodeId().value();
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
                new StandardMBean(new NodeMBean(node), NodeMXBean.class, true),
                nodeObjectName
            );
            mBeanServer.registerMBean(
                new StandardMBean(new StorageMBean(node), StorageMXBean.class, true),
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

    private record NodeMBean(Node node) implements NodeMXBean {
        @Override
        public String getNodeId() {
            return node.cluster().status().nodeId().value();
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
            return node.cluster().status().leader().map(NodeId::value).orElse("");
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
            return node.cluster().status().lastLeaderChange()
                .map(Instant::toEpochMilli)
                .orElse(0L);
        }

        @Override
        public long getProposalCount() {
            return node.metrics().proposals();
        }

        @Override
        public long getProposalFailureCount() {
            return node.metrics().failedProposals();
        }
    }

    private record StorageMBean(Node node) implements StorageMXBean {
        @Override
        public long getActiveMemtableBytes() {
            return node.metrics().storage().activeMemtableBytes();
        }

        @Override
        public int getImmutableMemtableCount() {
            return node.metrics().storage().immutableMemtableCount();
        }

        @Override
        public int getSstableCount() {
            return node.metrics().storage().sstableCount();
        }

        @Override
        public long getTotalSstableBytes() {
            return node.metrics().storage().totalSstableBytes();
        }

        @Override
        public int getActiveCompactions() {
            return node.metrics().storage().activeCompactions();
        }

        @Override
        public long getCompletedCompactions() {
            return node.metrics().storage().completedCompactions();
        }

        @Override
        public long getFailedCompactions() {
            return node.metrics().storage().failedCompactions();
        }

        @Override
        public long getLastCompactionDurationMillis() {
            return node.metrics().storage().lastCompactionDuration().toMillis();
        }

        @Override
        public long getCheckpointCount() {
            return node.metrics().storage().checkpointCount();
        }

        @Override
        public long getRestoreCount() {
            return node.metrics().storage().restoreCount();
        }

        @Override
        public long getLastCheckpointDurationMillis() {
            return node.metrics().storage().lastCheckpointDuration().toMillis();
        }

        @Override
        public long getLastRestoreDurationMillis() {
            return node.metrics().storage().lastRestoreDuration().toMillis();
        }
    }
}
