package io.partdb.node.cluster;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public record ClusterMembership(
    Set<String> voters,
    Set<String> learners
) {
    public ClusterMembership {
        voters = normalizeIds(voters, "voters");
        learners = normalizeIds(learners, "learners");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters must not be empty");
        }
        if (!disjoint(voters, learners)) {
            throw new IllegalArgumentException("node cannot be both voter and learner");
        }
    }

    public static ClusterMembership ofVoters(String... voters) {
        Objects.requireNonNull(voters, "voters must not be null");
        return new ClusterMembership(Set.of(voters), Set.of());
    }

    public boolean isVoter(String nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isLearner(String nodeId) {
        return learners.contains(nodeId);
    }

    public boolean isMember(String nodeId) {
        return isVoter(nodeId) || isLearner(nodeId);
    }

    public Set<String> memberIds() {
        var members = new LinkedHashSet<String>();
        members.addAll(voters);
        members.addAll(learners);
        return Set.copyOf(members);
    }

    public ClusterMembership addLearner(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (isMember(nodeId)) {
            throw new IllegalArgumentException("node already a member: " + nodeId);
        }
        var updatedLearners = new LinkedHashSet<>(learners);
        updatedLearners.add(nodeId);
        return new ClusterMembership(voters, updatedLearners);
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
