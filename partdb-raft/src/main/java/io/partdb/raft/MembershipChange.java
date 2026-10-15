package io.partdb.raft;

import java.util.Objects;

public sealed interface MembershipChange {
    NodeId node();

    record AddLearner(NodeId node) implements MembershipChange {
        public AddLearner {
            Objects.requireNonNull(node, "node must not be null");
        }
    }

    record PromoteToVoter(NodeId node) implements MembershipChange {
        public PromoteToVoter {
            Objects.requireNonNull(node, "node must not be null");
        }
    }

    record DemoteToLearner(NodeId node) implements MembershipChange {
        public DemoteToLearner {
            Objects.requireNonNull(node, "node must not be null");
        }
    }

    record RemoveNode(NodeId node) implements MembershipChange {
        public RemoveNode {
            Objects.requireNonNull(node, "node must not be null");
        }
    }
}
