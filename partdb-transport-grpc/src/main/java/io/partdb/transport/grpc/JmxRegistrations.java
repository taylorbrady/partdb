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
        this.nodeObjectName = objectName("Node", node.nodeId());
        this.storageObjectName = objectName("Storage", node.nodeId());
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
            return node.nodeId();
        }

        @Override
        public boolean isRunning() {
            return node.status().running();
        }

        @Override
        public String getRole() {
            return node.status().role().name();
        }

        @Override
        public String getLeaderId() {
            return node.leaderId().orElse("");
        }

        @Override
        public long getCurrentTerm() {
            return node.status().term();
        }

        @Override
        public long getCommitIndex() {
            return node.status().commitIndex();
        }

        @Override
        public long getAppliedIndex() {
            return node.status().lastAppliedIndex();
        }

        @Override
        public long getLastLeaderChangeEpochMillis() {
            return node.lastLeaderChangeEpochMillis();
        }

        @Override
        public long getProposalCount() {
            return node.proposalCount();
        }

        @Override
        public long getProposalFailureCount() {
            return node.proposalFailureCount();
        }
    }

    private record StorageMBean(PartDbNode node) implements PartDbStorageMXBean {
        @Override
        public long getActiveMemtableBytes() {
            return node.storageActiveMemtableBytes();
        }

        @Override
        public int getImmutableMemtableCount() {
            return node.storageImmutableMemtableCount();
        }

        @Override
        public int getSstableCount() {
            return node.storageSstableCount();
        }

        @Override
        public long getTotalSstableBytes() {
            return node.storageTotalSstableBytes();
        }

        @Override
        public int getActiveCompactions() {
            return node.storageActiveCompactions();
        }

        @Override
        public long getCompletedCompactions() {
            return node.storageCompletedCompactions();
        }

        @Override
        public long getFailedCompactions() {
            return node.storageFailedCompactions();
        }

        @Override
        public long getLastCompactionDurationMillis() {
            return node.storageLastCompactionDurationMillis();
        }

        @Override
        public long getCheckpointCount() {
            return node.storageCheckpointCount();
        }

        @Override
        public long getRestoreCount() {
            return node.storageRestoreCount();
        }

        @Override
        public long getLastCheckpointDurationMillis() {
            return node.storageLastCheckpointDurationMillis();
        }

        @Override
        public long getLastRestoreDurationMillis() {
            return node.storageLastRestoreDurationMillis();
        }
    }
}
