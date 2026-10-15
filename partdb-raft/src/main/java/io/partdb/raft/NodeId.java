package io.partdb.raft;

import java.util.Objects;
import java.util.regex.Pattern;

public record NodeId(String value) implements Comparable<NodeId> {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,62}");

    public NodeId {
        Objects.requireNonNull(value, "node id must not be null");
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                "node id must be 1-63 characters of letters, digits, '.', '_' or '-' and start with a letter or digit: " + value);
        }
    }

    public static NodeId of(String value) {
        return new NodeId(value);
    }

    @Override
    public int compareTo(NodeId other) {
        return value.compareTo(other.value);
    }

    @Override
    public String toString() {
        return value;
    }
}
