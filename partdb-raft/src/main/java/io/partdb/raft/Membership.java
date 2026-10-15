package io.partdb.raft;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

public record Membership(SortedSet<NodeId> voters, SortedSet<NodeId> learners) {
    public Membership {
        voters = normalize(voters, "voters");
        learners = normalize(learners, "learners");
        if (voters.isEmpty()) {
            throw new IllegalArgumentException("voters must not be empty");
        }
        if (!Collections.disjoint(voters, learners)) {
            throw new IllegalArgumentException("a node cannot be both voter and learner");
        }
    }

    public static Membership of(Set<NodeId> voters, Set<NodeId> learners) {
        return new Membership(new TreeSet<>(voters), new TreeSet<>(learners));
    }

    public static Membership ofVoters(Set<NodeId> voters) {
        return of(voters, Set.of());
    }

    public boolean isVoter(NodeId node) {
        return voters.contains(node);
    }

    public boolean isLearner(NodeId node) {
        return learners.contains(node);
    }

    public boolean isMember(NodeId node) {
        return isVoter(node) || isLearner(node);
    }

    public int quorum() {
        return voters.size() / 2 + 1;
    }

    public SortedSet<NodeId> members() {
        var members = new TreeSet<>(voters);
        members.addAll(learners);
        return Collections.unmodifiableSortedSet(members);
    }

    public Optional<String> check(MembershipChange change) {
        Objects.requireNonNull(change, "change must not be null");
        NodeId node = change.node();
        return switch (change) {
            case MembershipChange.AddLearner _ when isMember(node) -> Optional.of(node + " is already a member");
            case MembershipChange.PromoteToVoter _ when !isLearner(node) -> Optional.of(node + " must be a learner to be promoted");
            case MembershipChange.DemoteToLearner _ when !isVoter(node) -> Optional.of(node + " must be a voter to be demoted");
            case MembershipChange.DemoteToLearner _ when voters.size() == 1 -> Optional.of("cannot demote the last voter");
            case MembershipChange.RemoveNode _ when !isMember(node) -> Optional.of(node + " is not a member");
            case MembershipChange.RemoveNode _ when voters.equals(Set.of(node)) -> Optional.of("cannot remove the last voter");
            default -> Optional.empty();
        };
    }

    public Membership apply(MembershipChange change) {
        check(change).ifPresent(problem -> {
            throw new IllegalArgumentException(problem);
        });
        NodeId node = change.node();
        var nextVoters = new TreeSet<>(voters);
        var nextLearners = new TreeSet<>(learners);
        switch (change) {
            case MembershipChange.AddLearner _ -> nextLearners.add(node);
            case MembershipChange.PromoteToVoter _ -> {
                nextLearners.remove(node);
                nextVoters.add(node);
            }
            case MembershipChange.DemoteToLearner _ -> {
                nextVoters.remove(node);
                nextLearners.add(node);
            }
            case MembershipChange.RemoveNode _ -> {
                nextVoters.remove(node);
                nextLearners.remove(node);
            }
        }
        return new Membership(nextVoters, nextLearners);
    }

    private static SortedSet<NodeId> normalize(Set<NodeId> ids, String name) {
        Objects.requireNonNull(ids, name + " must not be null");
        var normalized = new TreeSet<NodeId>();
        for (NodeId id : ids) {
            normalized.add(Objects.requireNonNull(id, name + " must not contain null"));
        }
        return Collections.unmodifiableSortedSet(normalized);
    }
}
