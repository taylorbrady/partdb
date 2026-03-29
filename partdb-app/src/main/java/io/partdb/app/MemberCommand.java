package io.partdb.app;

import io.partdb.client.ClusterClient;
import io.partdb.protocol.cluster.proto.ClusterProto.Member;
import io.partdb.protocol.cluster.proto.ClusterProto.MemberListResponse;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

final class MemberCommand {

    private static final String DEFAULT_ENDPOINT = "localhost:8101";
    private static final long TIMEOUT_SECONDS = 30;

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
                printListUsage(out);
                return 0;
            } else {
                err.println("Error: unknown option: " + arg);
                return 1;
            }
        }

        try (var client = new ClusterClient(endpoint, TIMEOUT_SECONDS * 1000)) {
            MemberListResponse response = client.memberList().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

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

    private static void printText(MemberListResponse response, PrintStream out) {
        out.printf("%-12s %-24s %-8s %-8s%n", "NODE ID", "ADDRESS", "ROLE", "STATUS");
        for (Member member : response.getMembersList()) {
            String status = member.getIsLeader() ? "leader" : (member.getIsSelf() ? "self" : "");
            out.printf("%-12s %-24s %-8s %-8s%n",
                member.getNodeId(),
                member.getAddress().isEmpty() ? "(unknown)" : member.getAddress(),
                member.getRole().name().toLowerCase(),
                status);
        }
    }

    private static void printJson(MemberListResponse response, PrintStream out) {
        out.print("{\"leaderId\":");
        out.print(response.getLeaderId().isEmpty() ? "null" : "\"" + response.getLeaderId() + "\"");
        out.print(",\"members\":[");
        var members = response.getMembersList();
        for (int i = 0; i < members.size(); i++) {
            Member member = members.get(i);
            if (i > 0) {
                out.print(",");
            }
            out.print("{\"nodeId\":\"" + member.getNodeId() + "\"");
            out.print(",\"address\":"
                + (member.getAddress().isEmpty() ? "null" : "\"" + member.getAddress() + "\""));
            out.print(",\"role\":\"" + member.getRole().name().toLowerCase() + "\"");
            out.print(",\"isLeader\":" + member.getIsLeader());
            out.print(",\"isSelf\":" + member.getIsSelf() + "}");
        }
        out.println("]}");
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
        out.println("  -e, --endpoint <host:port>  Server endpoint (default: localhost:8101)");
        out.println("  -o, --output <format>       Output format: text, json (default: text)");
        out.println("  -h, --help                  Show this help message");
    }
}
