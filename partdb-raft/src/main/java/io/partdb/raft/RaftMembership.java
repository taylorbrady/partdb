package io.partdb.raft;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public record RaftMembership(SortedSet<String> voters, SortedSet<String> learners) {
    public RaftMembership {
        voters = normalize(voters, "voters");
        learners = normalize(learners, "learners");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters must not be empty");
        }
        if (!Collections.disjoint(voters, learners)) {
            throw new IllegalArgumentException("a node cannot be both voter and learner");
        }
    }

    public static RaftMembership of(Set<String> voters, Set<String> learners) {
        return new RaftMembership(new TreeSet<>(voters), new TreeSet<>(learners));
    }

    public static RaftMembership voters(String... voters) {
        return of(Set.of(voters), Set.of());
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

    public int quorum() {
        return voters.size() / 2 + 1;
    }

    public SortedSet<String> members() {
        var members = new TreeSet<>(voters);
        members.addAll(learners);
        return Collections.unmodifiableSortedSet(members);
    }

    public RaftMembership apply(MembershipChange change) {
        Objects.requireNonNull(change, "change must not be null");
        String node = change.nodeId();
        var nextVoters = new TreeSet<>(voters);
        var nextLearners = new TreeSet<>(learners);
        switch (change) {
            case MembershipChange.AddLearner _ -> {
                require(!isMember(node), "node is already a member: " + node);
                nextLearners.add(node);
            }
            case MembershipChange.PromoteToVoter _ -> {
                require(isLearner(node), "node must be a learner to promote: " + node);
                nextLearners.remove(node);
                nextVoters.add(node);
            }
            case MembershipChange.DemoteToLearner _ -> {
                require(isVoter(node), "node must be a voter to demote: " + node);
                nextVoters.remove(node);
                nextLearners.add(node);
            }
            case MembershipChange.RemoveNode _ -> {
                require(isMember(node), "node is not a member: " + node);
                nextVoters.remove(node);
                nextLearners.remove(node);
            }
        }
        require(!nextVoters.isEmpty(), "cannot remove the last voter");
        return new RaftMembership(nextVoters, nextLearners);
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }

    private static SortedSet<String> normalize(Set<String> ids, String name) {
        Objects.requireNonNull(ids, name + " must not be null");
        var normalized = new TreeSet<String>();
        for (String id : ids) {
            normalized.add(Validation.nodeId(id, name));
        }
        return Collections.unmodifiableSortedSet(normalized);
    }
}
