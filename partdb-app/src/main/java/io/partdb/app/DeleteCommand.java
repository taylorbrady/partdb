package io.partdb.app;

import io.partdb.client.KvClient;
import io.partdb.client.ServerEndpoint;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

record DeleteCommand(ServerEndpoint endpoint, String key) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb delete <key> [options]

        Delete a key.

        Options:
          -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)
          -h, --help                  Show this help message
        """;

    DeleteCommand {
        endpoint = Objects.requireNonNull(endpoint, "endpoint must not be null");
        key = Objects.requireNonNull(key, "key must not be null");
    }

    static AppCommand parse(Args args) {
        ServerEndpoint endpoint = CliParsing.DEFAULT_ENDPOINT;
        String key = null;

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
                    } else {
                        return new ErrorCommand("unexpected argument: " + arg, USAGE);
                    }
                }
            }
        }

        if (key == null) {
            return new ErrorCommand("key is required", USAGE);
        }
        return new DeleteCommand(endpoint, key);
    }

    @Override
    public int execute(CliRuntime runtime) {
        try (var client = new KvClient(runtime.kvClientConfig(endpoint))) {
            runtime.await(client.delete(key.getBytes(StandardCharsets.UTF_8)));
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
