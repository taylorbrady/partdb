package io.partdb.app;

import io.partdb.client.KvClient;
import io.partdb.client.ServerEndpoint;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

record PutCommand(ServerEndpoint endpoint, String key, String value) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb put <key> <value> [options]

        Put a key-value pair.

        Options:
          -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)
          -h, --help                  Show this help message
        """;

    PutCommand {
        endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        key = Objects.requireNonNull(key, "key must not be null");
        value = Objects.requireNonNull(value, "value must not be null");
    }

    static AppCommand parse(Args args) {
        ServerEndpoint endpoint = CliParsing.DEFAULT_ENDPOINT;
        String key = null;
        String value = null;

        while (args.hasNext()) {
            String arg = args.next();
            switch (arg) {
                case "--endpoint", "-e" -> endpoint = CliParsing.parseServerEndpoint(args.requireValue("--endpoint"));
                case "--help", "-h" -> {
                    return new HelpCommand(USAGE);
                }
                default -> {
                    if (arg.startsWith("-")) {
                        return new ErrorCommand("unknown option: " + arg, USAGE);
                    }
                    if (key == null) {
                        key = arg;
                    } else if (value == null) {
                        value = arg;
                    } else {
                        return new ErrorCommand("unexpected argument: " + arg, USAGE);
                    }
                }
            }
        }

        if (key == null || value == null) {
            return new ErrorCommand("key and value are required", USAGE);
        }
        return new PutCommand(endpoint, key, value);
    }

    @Override
    public int execute(CliRuntime runtime) {
        try (var client = new KvClient(runtime.kvClientConfig(endpoint))) {
            runtime.await(client.put(
                key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8)
            ));
            runtime.out().println("OK");
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
}
