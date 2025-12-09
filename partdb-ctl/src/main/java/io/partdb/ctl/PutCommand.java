package io.partdb.ctl;

import io.partdb.client.KvClient;
import io.partdb.client.KvClientConfig;
import io.partdb.common.ByteArray;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class PutCommand {

    private static final String DEFAULT_ENDPOINT = "localhost:8101";
    private static final long TIMEOUT_SECONDS = 30;

    static int run(String[] args, PrintStream out, PrintStream err) {
        String key = null;
        String value = null;
        String endpoint = DEFAULT_ENDPOINT;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equals("--endpoint") || arg.equals("-e")) {
                if (i + 1 >= args.length) {
                    err.println("Error: --endpoint requires a value");
                    return 1;
                }
                endpoint = args[++i];
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

        var config = KvClientConfig.defaultConfig(endpoint);
        try (var client = new KvClient(config)) {
            ByteArray keyBytes = ByteArray.copyOf(key.getBytes(StandardCharsets.UTF_8));
            ByteArray valueBytes = ByteArray.copyOf(value.getBytes(StandardCharsets.UTF_8));
            client.put(keyBytes, valueBytes).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            out.println("OK");
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

    private static String getRootCauseMessage(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String message = cause.getMessage();
        return message != null ? message : cause.getClass().getSimpleName();
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdbctl put <key> <value> [options]");
        out.println();
        out.println("Put a key-value pair.");
        out.println();
        out.println("Options:");
        out.println("  -e, --endpoint <host:port>  Server endpoint (default: localhost:8101)");
        out.println("  -h, --help                  Show this help message");
    }
}
