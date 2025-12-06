package io.partdb.raft;

public sealed interface ConfigChange {
    String nodeId();

    record AddLearner(String nodeId) implements ConfigChange {
        public AddLearner {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be null or blank");
            }
        }
    }

    record PromoteVoter(String nodeId) implements ConfigChange {
        public PromoteVoter {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be null or blank");
            }
        }
    }

    record DemoteToLearner(String nodeId) implements ConfigChange {
        public DemoteToLearner {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be null or blank");
            }
        }
    }

    record RemoveNode(String nodeId) implements ConfigChange {
        public RemoveNode {
            if (nodeId == null || nodeId.isBlank()) {
                throw new IllegalArgumentException("nodeId must not be null or blank");
            }
        }
    }
}
