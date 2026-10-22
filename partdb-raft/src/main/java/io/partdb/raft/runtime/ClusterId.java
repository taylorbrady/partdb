package io.partdb.raft.runtime;

import java.util.Objects;
import java.util.regex.Pattern;

public record ClusterId(String value) {

    private static final Pattern VALID = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,62}");

    public ClusterId {
        Objects.requireNonNull(value, "cluster id must not be null");
        if (!VALID.matcher(value).matches()) {
            throw new IllegalArgumentException(
                "cluster id must be 1-63 characters of letters, digits, '.', '_' or '-' and start with a letter or digit: " + value);
        }
    }

    public static ClusterId of(String value) {
        return new ClusterId(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
