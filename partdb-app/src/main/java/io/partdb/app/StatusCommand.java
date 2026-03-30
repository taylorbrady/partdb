package io.partdb.app;

import io.partdb.client.ClusterStatus;
import io.partdb.client.ClusterClient;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class StatusCommand {

    static int run(String[] args, PrintStream out, PrintStream err) {
        String endpoint = CliSupport.DEFAULT_ENDPOINT_TEXT;
        OutputFormat format = OutputFormat.TEXT;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--endpoint") || arg.equals("-e")) {
                endpoint = CliSupport.requireValue(args, ++i, "--endpoint");
            } else if (arg.equals("-o") || arg.equals("--output")) {
                try {
                    format = CliSupport.parseOutputFormat(CliSupport.requireValue(args, ++i, "--output"));
                } catch (IllegalArgumentException e) {
                    err.println("Error: " + e.getMessage());
                    return 1;
                }
            } else if (arg.equals("--help") || arg.equals("-h")) {
                printUsage(out);
                return 0;
            } else {
                err.println("Error: unknown option: " + arg);
                return 1;
            }
        }

        try (var client = new ClusterClient(CliSupport.defaultClusterClientConfig(endpoint))) {
            ClusterStatus response = client.status().get(CliSupport.REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (format == OutputFormat.JSON) {
                printJson(response, out);
            } else {
                printText(response, out);
            }
            return 0;
        } catch (TimeoutException e) {
            err.println("Error: request timed out after " + CliSupport.REQUEST_TIMEOUT_SECONDS + " seconds");
            return 1;
        } catch (ExecutionException e) {
            err.println("Error: " + CliSupport.rootCauseMessage(e));
            return 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            err.println("Error: operation interrupted");
            return 1;
        } catch (Exception e) {
            err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static void printText(ClusterStatus response, PrintStream out) {
        out.println("Node ID:        " + response.nodeId());
        out.println("Role:           " + response.role().name());
        out.println("Term:           " + response.term());
        out.println("Leader:         " + response.leaderId().orElse("(none)"));
        out.println("Commit Index:   " + response.commitIndex());
        out.println("Applied Index:  " + response.lastAppliedIndex());
        out.println("Running:        " + response.running());
    }

    private static void printJson(ClusterStatus response, PrintStream out) {
        out.println(toJson(response));
    }

    static String toJson(ClusterStatus response) {
        StringBuilder builder = new StringBuilder();
        builder.append('{');
        builder.append("\"nodeId\":").append(JsonOutput.quote(response.nodeId())).append(',');
        builder.append("\"role\":").append(JsonOutput.quote(response.role().name())).append(',');
        builder.append("\"term\":").append(response.term()).append(',');
        builder.append("\"leaderId\":")
            .append(response.leaderId().map(JsonOutput::quote).orElse("null"))
            .append(',');
        builder.append("\"commitIndex\":").append(response.commitIndex()).append(',');
        builder.append("\"lastAppliedIndex\":").append(response.lastAppliedIndex()).append(',');
        builder.append("\"isRunning\":").append(response.running());
        builder.append('}');
        return builder.toString();
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb status [options]");
        out.println();
        out.println("Show node status.");
        out.println();
        out.println("Options:");
        out.println("  -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)");
        out.println("  -o, --output <format>       Output format: text, json (default: text)");
        out.println("  -h, --help                  Show this help message");
    }
}
