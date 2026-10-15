package io.partdb.app;

import io.partdb.client.ServerEndpoint;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

record ClientOptions(List<ServerEndpoint> endpoints, OutputFormat output, Duration timeout) {

    static final String ENDPOINTS_VARIABLE = "PARTDB_ENDPOINTS";
    static final String DEFAULT_ENDPOINT = "localhost:8101";
    static final String USAGE = """
          -e, --endpoints <list>   Comma-separated server endpoints (default: $%s or %s)
              --timeout <seconds>  Request timeout in seconds (default: 10)
          -o, --output <format>    Output format: text or json (default: text)
          -h, --help               Show this help message
        """.formatted(ENDPOINTS_VARIABLE, DEFAULT_ENDPOINT);

    ClientOptions {
        endpoints = List.copyOf(endpoints);
        Objects.requireNonNull(output, "output must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        if (endpoints.isEmpty()) {
            throw new IllegalArgumentException("at least one endpoint is required");
        }
    }

    static final class Parser {
        private final Map<String, String> environment;
        private final List<ServerEndpoint> endpoints = new ArrayList<>();
        private OutputFormat output = OutputFormat.TEXT;
        private Duration timeout = Duration.ofSeconds(10);

        Parser(Map<String, String> environment) {
            this.environment = environment;
        }

        boolean accept(String option, Args args) {
            switch (option) {
                case "--endpoints", "--endpoint", "-e" -> endpoints.addAll(parseEndpoints(args.value(option)));
                case "--output", "-o" -> output = switch (args.value(option)) {
                    case "text" -> OutputFormat.TEXT;
                    case "json" -> OutputFormat.JSON;
                    case String other -> throw new IllegalArgumentException("unknown output format: " + other);
                };
                case "--timeout" -> {
                    int seconds = args.intValue(option);
                    if (seconds <= 0) {
                        throw new IllegalArgumentException("--timeout must be positive");
                    }
                    timeout = Duration.ofSeconds(seconds);
                }
                default -> {
                    return false;
                }
            }
            return true;
        }

        ClientOptions build() {
            if (endpoints.isEmpty()) {
                endpoints.addAll(parseEndpoints(environment.getOrDefault(ENDPOINTS_VARIABLE, DEFAULT_ENDPOINT)));
            }
            return new ClientOptions(endpoints, output, timeout);
        }

        private static List<ServerEndpoint> parseEndpoints(String value) {
            return Arrays.stream(value.split(",")).map(String::trim).filter(part -> !part.isEmpty()).map(ServerEndpoint::parse).toList();
        }
    }
}
