package io.partdb.raft;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

public record RaftConfiguration(Set<String> voters, Set<String> learners) {
    public RaftConfiguration {
        voters = normalizeIds(voters, "voters");
        learners = normalizeIds(learners, "learners");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters must not be empty");
        }
        if (!disjoint(voters, learners)) {
            throw new IllegalArgumentException("node cannot be both voter and learner");
        }
    }

    public static RaftConfiguration ofVoters(String... voters) {
        Objects.requireNonNull(voters, "voters must not be null");
        return new RaftConfiguration(Set.of(voters), Set.of());
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

    public int quorumSize() {
        return voters.size() / 2 + 1;
    }

    public Set<String> memberIds() {
        var members = new LinkedHashSet<String>();
        members.addAll(voters);
        members.addAll(learners);
        return Set.copyOf(members);
    }

    public Set<String> peerIdsExcluding(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        var peers = new LinkedHashSet<>(memberIds());
        peers.remove(nodeId);
        return Set.copyOf(peers);
    }

    public Set<String> votingPeerIdsExcluding(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        var peers = new LinkedHashSet<>(voters);
        peers.remove(nodeId);
        return Set.copyOf(peers);
    }

    public RaftConfiguration apply(ConfigurationChange change) {
        Objects.requireNonNull(change, "change must not be null");
        return switch (change) {
            case ConfigurationChange.AddLearner(var nodeId) -> addLearner(nodeId);
            case ConfigurationChange.PromoteToVoter(var nodeId) -> promoteToVoter(nodeId);
            case ConfigurationChange.DemoteToLearner(var nodeId) -> demoteToLearner(nodeId);
            case ConfigurationChange.RemoveNode(var nodeId) -> removeNode(nodeId);
        };
    }

    public RaftConfiguration addLearner(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (isMember(nodeId)) {
            throw new IllegalArgumentException("node already a member: " + nodeId);
        }
        var updatedLearners = new LinkedHashSet<>(learners);
        updatedLearners.add(nodeId);
        return new RaftConfiguration(voters, updatedLearners);
    }

    public RaftConfiguration promoteToVoter(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (!learners.contains(nodeId)) {
            throw new IllegalArgumentException("node must be a learner to promote: " + nodeId);
        }
        var updatedLearners = new LinkedHashSet<>(learners);
        updatedLearners.remove(nodeId);
        var updatedVoters = new LinkedHashSet<>(voters);
        updatedVoters.add(nodeId);
        return new RaftConfiguration(updatedVoters, updatedLearners);
    }

    public RaftConfiguration demoteToLearner(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (!voters.contains(nodeId)) {
            throw new IllegalArgumentException("node must be a voter to demote: " + nodeId);
        }
        if (voters.size() == 1) {
            throw new IllegalArgumentException("cannot demote the last voter");
        }
        var updatedVoters = new LinkedHashSet<>(voters);
        updatedVoters.remove(nodeId);
        var updatedLearners = new LinkedHashSet<>(learners);
        updatedLearners.add(nodeId);
        return new RaftConfiguration(updatedVoters, updatedLearners);
    }

    public RaftConfiguration removeNode(String nodeId) {
        requireNonBlank(nodeId, "nodeId");
        if (!isMember(nodeId)) {
            throw new IllegalArgumentException("node not a member: " + nodeId);
        }
        if (voters.contains(nodeId) && voters.size() == 1) {
            throw new IllegalArgumentException("cannot remove the last voter");
        }
        var updatedVoters = new LinkedHashSet<>(voters);
        updatedVoters.remove(nodeId);
        var updatedLearners = new LinkedHashSet<>(learners);
        updatedLearners.remove(nodeId);
        return new RaftConfiguration(updatedVoters.isEmpty() ? voters : updatedVoters, updatedLearners);
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
