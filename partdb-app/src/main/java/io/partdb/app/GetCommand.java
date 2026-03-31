package io.partdb.app;

import io.partdb.client.KvClient;
import io.partdb.client.ServerEndpoint;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

record GetCommand(ServerEndpoint endpoint, String key) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb kv get <key> [options]

        Get a value by key.

        Options:
          -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)
          -h, --help                  Show this help message
        """;

    GetCommand {
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
        return new GetCommand(endpoint, key);
    }

    @Override
    public int execute(CliRuntime runtime) {
        try (var client = new KvClient(runtime.kvClientConfig(endpoint))) {
            Optional<byte[]> result = runtime.await(client.get(key.getBytes(StandardCharsets.UTF_8)));
            if (result.isPresent()) {
                runtime.out().println(new String(result.get(), StandardCharsets.UTF_8));
                return 0;
            }
            runtime.err().println("(not found)");
            return 1;
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
