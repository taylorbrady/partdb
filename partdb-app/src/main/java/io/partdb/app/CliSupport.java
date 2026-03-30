package io.partdb.app;

import io.partdb.client.ClusterClientConfig;
import io.partdb.client.KvClientConfig;
import io.partdb.client.ServerEndpoint;

final class CliSupport {
    static final String DEFAULT_ENDPOINT_TEXT = "localhost:8101";
    static final long REQUEST_TIMEOUT_SECONDS = 30;

    private CliSupport() {}

    static ClusterClientConfig defaultClusterClientConfig(String endpointText) {
        return ClusterClientConfig.defaultConfig(parseServerEndpoint(endpointText));
    }

    static KvClientConfig defaultKvClientConfig(String endpointText) {
        return KvClientConfig.defaultConfig(parseServerEndpoint(endpointText));
    }

    static String requireValue(String[] args, int index, String optionName) {
        if (index >= args.length) {
            throw new IllegalArgumentException(optionName + " requires a value");
        }
        return args[index];
    }

    static int requireIntValue(String[] args, int index, String optionName) {
        String value = requireValue(args, index, optionName);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(optionName + " must be a valid integer, got: " + value);
        }
    }

    static OutputFormat parseOutputFormat(String value) {
        return switch (value) {
            case "text" -> OutputFormat.TEXT;
            case "json" -> OutputFormat.JSON;
            default -> throw new IllegalArgumentException("unknown output format: " + value);
        };
    }

    static String rootCauseMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message != null ? message : cause.getClass().getSimpleName();
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

    private static ServerEndpoint parseServerEndpoint(String value) {
        try {
            return ServerEndpoint.parse(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid endpoint: " + value, e);
        }
    }

    record NodeEndpoint(String nodeId, String endpoint) {}
}
