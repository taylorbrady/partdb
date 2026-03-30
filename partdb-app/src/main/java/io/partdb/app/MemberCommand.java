package io.partdb.app;

import io.partdb.client.ClusterClient;
import io.partdb.client.ClusterMember;
import io.partdb.client.ClusterMembership;

import java.io.PrintStream;
import java.util.Locale;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class MemberCommand {

    static int run(String[] args, PrintStream out, PrintStream err) {
        if (args.length == 0) {
            err.println("Error: subcommand required");
            err.println();
            printUsage(err);
            return 1;
        }

        String subcommand = args[0];
        String[] subArgs = Arrays.copyOfRange(args, 1, args.length);

        return switch (subcommand) {
            case "list" -> runList(subArgs, out, err);
            case "help", "--help", "-h" -> {
                printUsage(out);
                yield 0;
            }
            default -> {
                err.println("Error: unknown subcommand: " + subcommand);
                err.println();
                printUsage(err);
                yield 1;
            }
        };
    }

    private static int runList(String[] args, PrintStream out, PrintStream err) {
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
                printListUsage(out);
                return 0;
            } else {
                err.println("Error: unknown option: " + arg);
                return 1;
            }
        }

        try (var client = new ClusterClient(CliSupport.defaultClusterClientConfig(endpoint))) {
            ClusterMembership response = client.membership().get(CliSupport.REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);

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

    private static void printText(ClusterMembership response, PrintStream out) {
        out.printf("%-12s %-24s %-8s %-8s%n", "NODE ID", "RAFT ADDRESS", "ROLE", "STATUS");
        for (ClusterMember member : response.members()) {
            String status = member.leader() ? "leader" : (member.self() ? "self" : "");
            out.printf("%-12s %-24s %-8s %-8s%n",
                member.nodeId(),
                member.raftEndpoint().map(Object::toString).orElse("(unknown)"),
                member.role().name().toLowerCase(Locale.ROOT),
                status);
        }
    }

    private static void printJson(ClusterMembership response, PrintStream out) {
        out.println(toJson(response));
    }

    static String toJson(ClusterMembership response) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"leaderId\":");
        builder.append(response.leaderId().map(JsonOutput::quote).orElse("null"));
        builder.append(",\"members\":[");

        var members = response.members();
        for (int i = 0; i < members.size(); i++) {
            ClusterMember member = members.get(i);
            if (i > 0) {
                builder.append(',');
            }
            builder.append("{\"nodeId\":").append(JsonOutput.quote(member.nodeId()));
            builder.append(",\"raftAddress\":")
                .append(member.raftEndpoint().map(endpoint -> JsonOutput.quote(endpoint.toString())).orElse("null"));
            builder.append(",\"role\":")
                .append(JsonOutput.quote(member.role().name().toLowerCase(Locale.ROOT)));
            builder.append(",\"isLeader\":").append(member.leader());
            builder.append(",\"isSelf\":").append(member.self()).append('}');
        }
        builder.append("]}");
        return builder.toString();
    }

    private static void printUsage(PrintStream out) {
        out.println("Usage: partdb member <subcommand> [options]");
        out.println();
        out.println("Manage cluster members.");
        out.println();
        out.println("Subcommands:");
        out.println("  list    List cluster members");
        out.println("  help    Show this help message");
    }

    private static void printListUsage(PrintStream out) {
        out.println("Usage: partdb member list [options]");
        out.println();
        out.println("List cluster members.");
        out.println();
        out.println("Options:");
        out.println("  -e, --endpoint <endpoint>   Server endpoint (default: localhost:8101)");
        out.println("  -o, --output <format>       Output format: text, json (default: text)");
        out.println("  -h, --help                  Show this help message");
    }
}
