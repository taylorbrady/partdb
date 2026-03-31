package io.partdb.raft;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public record RaftMembership(Set<String> voters, Set<String> learners) {
    public RaftMembership {
        voters = normalizeIds(voters, "voters");
        learners = normalizeIds(learners, "learners");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("Must have at least one voter");
        }
        if (!disjoint(voters, learners)) {
            throw new IllegalArgumentException("Node cannot be both voter and learner");
        }
    }

    public static RaftMembership ofVoters(String... voters) {
        Objects.requireNonNull(voters, "voters must not be null");
        return new RaftMembership(Set.of(voters), Set.of());
    }

    public boolean isVoter(String id) {
        return voters.contains(id);
    }

    public boolean isLearner(String id) {
        return learners.contains(id);
    }

    public boolean isMember(String id) {
        return voters.contains(id) || learners.contains(id);
    }

    public RaftMembership addLearner(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (isMember(nodeId)) {
            throw new IllegalArgumentException("Node already a member: " + nodeId);
        }
        var newLearners = new LinkedHashSet<>(learners);
        newLearners.add(nodeId);
        return new RaftMembership(voters, newLearners);
    }

    public RaftMembership promoteVoter(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (!learners.contains(nodeId)) {
            throw new IllegalArgumentException("Node must be a learner to promote: " + nodeId);
        }
        var newLearners = new LinkedHashSet<>(learners);
        newLearners.remove(nodeId);
        var newVoters = new LinkedHashSet<>(voters);
        newVoters.add(nodeId);
        return new RaftMembership(newVoters, newLearners);
    }

    public RaftMembership demoteToLearner(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (!voters.contains(nodeId)) {
            throw new IllegalArgumentException("Node must be a voter to demote: " + nodeId);
        }
        if (voters.size() == 1) {
            throw new IllegalArgumentException("Cannot demote the last voter");
        }
        var newVoters = new LinkedHashSet<>(voters);
        newVoters.remove(nodeId);
        var newLearners = new LinkedHashSet<>(learners);
        newLearners.add(nodeId);
        return new RaftMembership(newVoters, newLearners);
    }

    public RaftMembership removeNode(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (!isMember(nodeId)) {
            throw new IllegalArgumentException("Node not a member: " + nodeId);
        }
        if (voters.contains(nodeId) && voters.size() == 1) {
            throw new IllegalArgumentException("Cannot remove the last voter");
        }
        var newVoters = new LinkedHashSet<>(voters);
        newVoters.remove(nodeId);
        var newLearners = new LinkedHashSet<>(learners);
        newLearners.remove(nodeId);
        return new RaftMembership(newVoters.isEmpty() ? voters : newVoters, newLearners);
    }

    private static Set<String> normalizeIds(Set<String> ids, String name) {
        Objects.requireNonNull(ids, name + " must not be null");
        var normalized = new LinkedHashSet<String>();
        for (String id : ids) {
            normalized.add(requireNonBlank(id, "memberId"));
        }
        return Set.copyOf(normalized);
    }

    private static boolean disjoint(Set<String> left, Set<String> right) {
        for (String id : left) {
            if (right.contains(id)) {
                return false;
            }
        }
        return true;
    }

    private static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
