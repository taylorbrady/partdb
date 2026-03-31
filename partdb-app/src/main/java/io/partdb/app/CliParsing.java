package io.partdb.app;

import io.partdb.client.ServerEndpoint;

import java.util.Objects;

final class CliParsing {
    static final String DEFAULT_ENDPOINT_TEXT = "localhost:8101";
    static final ServerEndpoint DEFAULT_ENDPOINT = ServerEndpoint.parse(DEFAULT_ENDPOINT_TEXT);

    private CliParsing() {}

    static OutputFormat parseOutputFormat(String value) {
        return switch (value) {
            case "text" -> OutputFormat.TEXT;
            case "json" -> OutputFormat.JSON;
            default -> throw new IllegalArgumentException("unknown output format: " + value);
        };
    }

    static ServerEndpoint parseServerEndpoint(String value) {
        try {
            return ServerEndpoint.parse(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid endpoint: " + value, e);
        }
    }

    static NodeEndpoint parseNodeEndpointSpec(String value, String optionName) {
        String[] parts = value.split("=", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                "Invalid " + optionName + " format: '" + value + "'. Expected: nodeId=endpoint"
            );
        }

        String nodeId = parts[0].trim();
        String endpoint = parts[1].trim();
        if (nodeId.isEmpty() || endpoint.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid " + optionName + " format: '" + value + "'. Expected: nodeId=endpoint"
            );
        }
        return new NodeEndpoint(nodeId, endpoint);
    }

    static String requireNonBlank(String value, String name) {
        Objects.requireNonNull(value, name + " must not be null");
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    record NodeEndpoint(String nodeId, String endpoint) {
        NodeEndpoint {
            nodeId = requireNonBlank(nodeId, "nodeId");
            endpoint = requireNonBlank(endpoint, "endpoint");
        }
    }
}
