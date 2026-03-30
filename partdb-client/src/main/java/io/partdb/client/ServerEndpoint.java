package io.partdb.client;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

public record ServerEndpoint(String host, int port) {
    public ServerEndpoint {
        Objects.requireNonNull(host, "host must not be null");
        if (host.isBlank()) {
            throw new IllegalArgumentException("host must not be blank");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port must be between 1 and 65535");
        }
    }

    public static ServerEndpoint parse(String value) {
        Objects.requireNonNull(value, "value must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException("endpoint must not be blank");
        }

        URI uri;
        try {
            uri = URI.create("partdb://" + value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid endpoint: " + value, e);
        }

        if (uri.getHost() == null || uri.getPort() < 0) {
            throw new IllegalArgumentException("Invalid endpoint: " + value);
        }
        if (uri.getRawUserInfo() != null) {
            throw new IllegalArgumentException("Invalid endpoint: " + value);
        }
        String path = uri.getRawPath();
        if (path != null && !path.isEmpty()) {
            throw new IllegalArgumentException("Invalid endpoint: " + value);
        }

        return new ServerEndpoint(normalizeHost(uri.getHost()), uri.getPort());
    }

    public static Optional<ServerEndpoint> tryParse(String value) {
        if (value == null || value.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(parse(value));
        } catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    @Override
    public String toString() {
        return host.contains(":")
            ? "[" + host + "]:" + port
            : host + ":" + port;
    }

    private static String normalizeHost(String host) {
        if (host.startsWith("[") && host.endsWith("]")) {
            return host.substring(1, host.length() - 1);
        }
        return host;
    }
}
