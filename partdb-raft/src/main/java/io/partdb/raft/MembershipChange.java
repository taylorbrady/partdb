package io.partdb.raft;

public sealed interface MembershipChange {
    String nodeId();

    record AddLearner(String nodeId) implements MembershipChange {
        public AddLearner {
            Validation.nodeId(nodeId, "nodeId");
        }
    }

    record PromoteToVoter(String nodeId) implements MembershipChange {
        public PromoteToVoter {
            Validation.nodeId(nodeId, "nodeId");
        }
    }

    record DemoteToLearner(String nodeId) implements MembershipChange {
        public DemoteToLearner {
            Validation.nodeId(nodeId, "nodeId");
        }
    }

    record RemoveNode(String nodeId) implements MembershipChange {
        public RemoveNode {
            Validation.nodeId(nodeId, "nodeId");
        }
    }
}
