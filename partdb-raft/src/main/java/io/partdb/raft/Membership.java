package io.partdb.raft;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public record Membership(Set<String> voters, Set<String> learners) {
    public Membership {
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("Must have at least one voter");
        }
        voters = Set.copyOf(voters);
        learners = Set.copyOf(learners);
        if (!Collections.disjoint(voters, learners)) {
            throw new IllegalArgumentException("Node cannot be both voter and learner");
        }
    }

    public static Membership ofVoters(String... voters) {
        return new Membership(Set.of(voters), Set.of());
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

    public Membership addLearner(String nodeId) {
        if (isMember(nodeId)) {
            throw new IllegalArgumentException("Node already a member: " + nodeId);
        }
        var newLearners = new HashSet<>(learners);
        newLearners.add(nodeId);
        return new Membership(voters, newLearners);
    }

    public Membership promoteVoter(String nodeId) {
        if (!learners.contains(nodeId)) {
            throw new IllegalArgumentException("Node must be a learner to promote: " + nodeId);
        }
        var newLearners = new HashSet<>(learners);
        newLearners.remove(nodeId);
        var newVoters = new HashSet<>(voters);
        newVoters.add(nodeId);
        return new Membership(newVoters, newLearners);
    }

    public Membership demoteToLearner(String nodeId) {
        if (!voters.contains(nodeId)) {
            throw new IllegalArgumentException("Node must be a voter to demote: " + nodeId);
        }
        if (voters.size() == 1) {
            throw new IllegalArgumentException("Cannot demote the last voter");
        }
        var newVoters = new HashSet<>(voters);
        newVoters.remove(nodeId);
        var newLearners = new HashSet<>(learners);
        newLearners.add(nodeId);
        return new Membership(newVoters, newLearners);
    }

    public Membership removeNode(String nodeId) {
        if (!isMember(nodeId)) {
            throw new IllegalArgumentException("Node not a member: " + nodeId);
        }
        if (voters.contains(nodeId) && voters.size() == 1) {
            throw new IllegalArgumentException("Cannot remove the last voter");
        }
        var newVoters = new HashSet<>(voters);
        newVoters.remove(nodeId);
        var newLearners = new HashSet<>(learners);
        newLearners.remove(nodeId);
        return new Membership(newVoters.isEmpty() ? voters : newVoters, newLearners);
    }
}
