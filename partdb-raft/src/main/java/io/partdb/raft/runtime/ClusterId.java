package io.partdb.raft.runtime;

import io.partdb.raft.Membership;
import io.partdb.raft.NodeId;

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.CRC32C;

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

    public static ClusterId derivedFrom(Membership membership) {
        String members = membership.voters().stream().map(NodeId::value).collect(Collectors.joining(","))
            + "|" + membership.learners().stream().map(NodeId::value).collect(Collectors.joining(","));
        var crc = new CRC32C();
        crc.update(members.getBytes(StandardCharsets.UTF_8));
        return new ClusterId("partdb-" + HexFormat.of().toHexDigits((int) crc.getValue()));
    }

    @Override
    public String toString() {
        return value;
    }
}
