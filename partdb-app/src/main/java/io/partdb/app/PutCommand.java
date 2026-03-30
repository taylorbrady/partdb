package io.partdb.app;

import io.partdb.client.KvClient;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class PutCommand {

    static int run(String[] args, PrintStream out, PrintStream err) {
        String key = null;
        String value = null;
        String endpoint = CliSupport.DEFAULT_ENDPOINT_TEXT;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--endpoint") || arg.equals("-e")) {
                endpoint = CliSupport.requireValue(args, ++i, "--endpoint");
            } else if (arg.equals("--help") || arg.equals("-h")) {
                printUsage(out);
                return 0;
            } else if (!arg.startsWith("-")) {
                if (key == null) {
                    key = arg;
                } else if (value == null) {
                    value = arg;
                } else {
                    err.println("Error: unexpected argument: " + arg);
                    return 1;
                }
            } else {
                err.println("Error: unknown option: " + arg);
                return 1;
            }
        }

        if (key == null || value == null) {
            err.println("Error: key and value are required");
            err.println();
            printUsage(err);
            return 1;
        }

        try (var client = new KvClient(CliSupport.defaultKvClientConfig(endpoint))) {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            client.put(keyBytes, valueBytes).get(CliSupport.REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            out.println("OK");
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

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb put <key> <value> [options]");
        out.println();
        out.println("Put a key-value pair.");
        out.println();
        out.println("Options:");
        out.println("  -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)");
        out.println("  -h, --help                  Show this help message");
    }
}
