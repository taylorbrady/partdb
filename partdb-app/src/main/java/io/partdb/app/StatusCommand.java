package io.partdb.app;

import io.partdb.client.ClusterStatus;
import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterClientConfig;

import java.io.PrintStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class StatusCommand {

    private static final String DEFAULT_ENDPOINT = "localhost:8101";
    private static final long TIMEOUT_SECONDS = 30;

    static int run(String[] args, PrintStream out, PrintStream err) {
        String endpoint = DEFAULT_ENDPOINT;
        OutputFormat format = OutputFormat.TEXT;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--endpoint") || arg.equals("-e")) {
                if (i + 1 >= args.length) {
                    err.println("Error: --endpoint requires a value");
                    return 1;
                }
                endpoint = args[++i];
            } else if (arg.equals("-o") || arg.equals("--output")) {
                if (i + 1 >= args.length) {
                    err.println("Error: -o requires a value");
                    return 1;
                }
                String formatStr = args[++i];
                if (formatStr.equals("json")) {
                    format = OutputFormat.JSON;
                } else if (!formatStr.equals("text")) {
                    err.println("Error: unknown output format: " + formatStr);
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

        try (var client = new ClusterClient(ClusterClientConfig.defaultConfig(endpoint))) {
            ClusterStatus response = client.status().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (format == OutputFormat.JSON) {
                printJson(response, out);
            } else {
                printText(response, out);
            }
            return 0;
        } catch (TimeoutException e) {
            err.println("Error: request timed out after " + TIMEOUT_SECONDS + " seconds");
            return 1;
        } catch (ExecutionException e) {
            err.println("Error: " + getRootCauseMessage(e));
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
        out.print("{");
        out.print("\"nodeId\":\"" + response.nodeId() + "\",");
        out.print("\"role\":\"" + response.role().name() + "\",");
        out.print("\"term\":" + response.term() + ",");
        out.print("\"leaderId\":"
            + response.leaderId().map(id -> "\"" + id + "\"").orElse("null") + ",");
        out.print("\"commitIndex\":" + response.commitIndex() + ",");
        out.print("\"lastAppliedIndex\":" + response.lastAppliedIndex() + ",");
        out.print("\"isRunning\":" + response.running());
        out.println("}");
    }

    private static String getRootCauseMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message != null ? message : cause.getClass().getSimpleName();
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb status [options]");
        out.println();
        out.println("Show node status.");
        out.println();
        out.println("Options:");
        out.println("  -e, --endpoint <host:port>  Server endpoint (default: localhost:8101)");
        out.println("  -o, --output <format>       Output format: text, json (default: text)");
        out.println("  -h, --help                  Show this help message");
    }
}
