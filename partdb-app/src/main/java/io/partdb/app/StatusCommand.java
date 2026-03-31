package io.partdb.app;

import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterStatus;
import io.partdb.client.ServerEndpoint;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

record StatusCommand(ServerEndpoint endpoint, OutputFormat format) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb cluster status [options]

        Show node status.

        Options:
          -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)
          -o, --output <format>       Output format: text, json (default: text)
          -h, --help                  Show this help message
        """;

    StatusCommand {
        endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        format = Objects.requireNonNull(format, "format must not be null");
    }

    static AppCommand parse(Args args) {
        ServerEndpoint endpoint = CliParsing.DEFAULT_ENDPOINT;
        OutputFormat format = OutputFormat.TEXT;

        while (args.hasNext()) {
            String arg = args.next();
            try {
                switch (arg) {
                    case "--endpoint", "-e" -> endpoint = CliParsing.parseServerEndpoint(args.requireValue("--endpoint"));
                    case "--output", "-o" -> format = CliParsing.parseOutputFormat(args.requireValue("--output"));
                    case "--help", "-h" -> {
                        return new HelpCommand(USAGE);
                    }
                    default -> {
                        return new ErrorCommand("unknown option: " + arg, USAGE);
                    }
                }
            } catch (IllegalArgumentException e) {
                return new ErrorCommand(e.getMessage(), USAGE);
            }
        }

        return new StatusCommand(endpoint, format);
    }

    @Override
    public int execute(CliRuntime runtime) {
        try (var client = new ClusterClient(runtime.clusterClientConfig(endpoint))) {
            ClusterStatus response = runtime.await(client.status());
            if (format == OutputFormat.JSON) {
                runtime.out().println(toJson(response));
            } else {
                printText(response, runtime);
            }
            return 0;
        } catch (TimeoutException e) {
            return runtime.timeout();
        } catch (ExecutionException e) {
            return runtime.error(CliRuntime.rootCauseMessage(e));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return runtime.error("operation interrupted");
        } catch (Exception e) {
            return runtime.error(e.getMessage());
        }
    }

    private static void printText(ClusterStatus response, CliRuntime runtime) {
        runtime.out().println("Node ID:        " + response.nodeId());
        runtime.out().println("Role:           " + response.role().name());
        runtime.out().println("Term:           " + response.term());
        runtime.out().println("Leader:         " + response.leaderId().orElse("(none)"));
        runtime.out().println("Commit Index:   " + response.commitIndex());
        runtime.out().println("Applied Index:  " + response.lastAppliedIndex());
        runtime.out().println("Running:        " + response.running());
    }

    static String toJson(ClusterStatus response) {
        return JsonWriter.object(json -> {
            json.field("nodeId", response.nodeId());
            json.field("role", response.role().name());
            json.field("term", response.term());
            json.field("leaderId", response.leaderId());
            json.field("commitIndex", response.commitIndex());
            json.field("lastAppliedIndex", response.lastAppliedIndex());
            json.field("isRunning", response.running());
        });
    }
}
