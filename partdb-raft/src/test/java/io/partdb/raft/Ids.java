package io.partdb.raft;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public final class Ids {

    private Ids() {
    }

    public static NodeId id(String value) {
        return new NodeId(value);
    }

    public static Set<NodeId> ids(String... values) {
        return Arrays.stream(values).map(NodeId::new).collect(Collectors.toSet());
    }

    public static Set<NodeId> ids(Set<String> values) {
        return values.stream().map(NodeId::new).collect(Collectors.toSet());
    }

    public static Membership voters(String... voters) {
        return Membership.ofVoters(ids(voters));
    }

    public static Membership membership(Set<String> voters, Set<String> learners) {
        return Membership.of(ids(voters), ids(learners));
    }
}
