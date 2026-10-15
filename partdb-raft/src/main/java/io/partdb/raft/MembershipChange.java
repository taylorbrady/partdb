package io.partdb.raft;

public sealed interface MembershipChange {
    String nodeId();

    default boolean revokesVote(String voter) {
        return nodeId().equals(voter) && switch (this) {
            case RemoveNode _, DemoteToLearner _ -> true;
            case AddLearner _, PromoteToVoter _ -> false;
        };
    }

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
