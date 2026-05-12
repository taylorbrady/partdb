package io.partdb.raft;

public sealed interface MembershipChange {
    String nodeId();

    record AddLearner(String nodeId) implements MembershipChange {
        public AddLearner {
            nodeId = requireNodeId(nodeId);
        }
    }

    record PromoteToVoter(String nodeId) implements MembershipChange {
        public PromoteToVoter {
            nodeId = requireNodeId(nodeId);
        }
    }

    record DemoteToLearner(String nodeId) implements MembershipChange {
        public DemoteToLearner {
            nodeId = requireNodeId(nodeId);
        }
    }

    record RemoveNode(String nodeId) implements MembershipChange {
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
