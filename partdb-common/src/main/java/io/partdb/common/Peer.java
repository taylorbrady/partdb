package io.partdb.common;

import java.util.Objects;

public record Peer(String nodeId, String host, int port) {

    public Peer {
        Objects.requireNonNull(nodeId, "nodeId must not be null");
        Objects.requireNonNull(host, "host must not be null");
        if (nodeId.isBlank()) {
            throw new IllegalArgumentException("nodeId must not be blank");
        }
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }

    public String address() {
        return host + ":" + port;
    }

    public static Peer parse(String spec) {
        int eq = spec.indexOf('=');
        if (eq <= 0) {
            throw new IllegalArgumentException(
                "Peer must be in format nodeId=host:port, got: " + spec);
        }
        String nodeId = spec.substring(0, eq);
        String address = spec.substring(eq + 1);

        int colon = address.lastIndexOf(':');
        if (colon <= 0) {
            throw new IllegalArgumentException(
                "Peer address must be in format host:port, got: " + address);
        }
        String host = address.substring(0, colon);
        int port;
        try {
            port = Integer.parseInt(address.substring(colon + 1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "Peer port must be a valid integer, got: " + address.substring(colon + 1));
        }

        return new Peer(nodeId, host, port);
    }
}
