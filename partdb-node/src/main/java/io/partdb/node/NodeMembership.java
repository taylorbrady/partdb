package io.partdb.node;

import io.partdb.raft.RaftMembership;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public record NodeMembership(
    Set<String> voters,
    Set<String> learners
) {
    public NodeMembership {
        voters = normalizeIds(voters, "voters");
        learners = normalizeIds(learners, "learners");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("Must have at least one voter");
        }
        if (!disjoint(voters, learners)) {
            throw new IllegalArgumentException("Node cannot be both voter and learner");
        }
    }

    public static NodeMembership ofVoters(String... voters) {
        Objects.requireNonNull(voters, "voters must not be null");
        return new NodeMembership(Set.of(voters), Set.of());
    }

    public boolean isVoter(String nodeId) {
        return voters.contains(nodeId);
    }

    public boolean isLearner(String nodeId) {
        return learners.contains(nodeId);
    }

    public boolean isMember(String nodeId) {
        return voters.contains(nodeId) || learners.contains(nodeId);
    }

    public Set<String> memberIds() {
        var members = new LinkedHashSet<String>();
        members.addAll(voters);
        members.addAll(learners);
        return Set.copyOf(members);
    }

    public NodeMembership addLearner(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (isMember(nodeId)) {
            throw new IllegalArgumentException("Node already a member: " + nodeId);
        }
        var updatedLearners = new LinkedHashSet<>(learners);
        updatedLearners.add(nodeId);
        return new NodeMembership(voters, updatedLearners);
    }

    static NodeMembership fromRaftMembership(RaftMembership membership) {
        return new NodeMembership(membership.voters(), membership.learners());
    }

    RaftMembership toRaftMembership() {
        return new RaftMembership(voters, learners);
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
