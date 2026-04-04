package io.partdb.raft;

public sealed interface ConfigurationChange {
    String nodeId();

    record AddLearner(String nodeId) implements ConfigurationChange {
        public AddLearner {
            nodeId = requireNodeId(nodeId);
        }
    }

    record PromoteToVoter(String nodeId) implements ConfigurationChange {
        public PromoteToVoter {
            nodeId = requireNodeId(nodeId);
        }
    }

    record DemoteToLearner(String nodeId) implements ConfigurationChange {
        public DemoteToLearner {
            nodeId = requireNodeId(nodeId);
        }
    }

    record RemoveNode(String nodeId) implements ConfigurationChange {
        public RemoveNode {
            nodeId = requireNodeId(nodeId);
        }
    }

    private static String requireNodeId(String nodeId) {
        if (nodeId == null || nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be null or blank");
        }
        return nodeId;
    }
}
