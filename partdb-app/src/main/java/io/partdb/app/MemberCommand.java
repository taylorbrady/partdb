package io.partdb.app;

import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterMember;
import io.partdb.client.ClusterMembership;
import io.partdb.client.ServerEndpoint;

import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

record MemberCommand(ServerEndpoint endpoint, OutputFormat format) implements AppCommand {
    private static final String USAGE = """
        Usage: partdb cluster members [options]

        List cluster members.

        Options:
          -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)
          -o, --output <format>       Output format: text, json (default: text)
          -h, --help                  Show this help message
        """;

    MemberCommand {
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

        return new MemberCommand(endpoint, format);
    }

    @Override
    public int execute(CliRuntime runtime) {
        try (var client = new ClusterClient(runtime.clusterClientConfig(endpoint))) {
            ClusterMembership response = runtime.await(client.membership());
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

    private static void printText(ClusterMembership response, CliRuntime runtime) {
        runtime.out().printf("%-12s %-8s %-8s%n", "NODE ID", "ROLE", "STATUS");
        for (ClusterMember member : response.members()) {
            String status = member.leader() ? "leader" : (member.self() ? "self" : "");
            runtime.out().printf("%-12s %-8s %-8s%n",
                member.nodeId(),
                member.role().name().toLowerCase(Locale.ROOT),
                status);
        }
    }

    static String toJson(ClusterMembership response) {
        return JsonWriter.object(json -> {
            json.field("leaderId", response.leaderId());
            json.array("members", response.members(), (array, member) -> array.object(memberJson -> {
                memberJson.field("nodeId", member.nodeId());
                memberJson.field("role", member.role().name().toLowerCase(Locale.ROOT));
                memberJson.field("isLeader", member.leader());
                memberJson.field("isSelf", member.self());
            }));
        });
    }
}
