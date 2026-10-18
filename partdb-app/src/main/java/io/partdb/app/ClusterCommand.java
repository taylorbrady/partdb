package io.partdb.app;

import io.partdb.client.Member;
import io.partdb.client.Membership;
import io.partdb.client.NodeStatus;
import io.partdb.client.PartDbClient;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

record ClusterCommand(Query query, ClientOptions options) implements ClientCommand {

    static final String USAGE = """
        Usage: partdb cluster <command>

        Commands:
          status     Show the status of the first endpoint's node
          members    List cluster members
        """;

    private static final String STATUS_USAGE = """
        Usage: partdb cluster status [options]

        Options:
        """ + ClientOptions.USAGE;

    private static final String MEMBERS_USAGE = """
        Usage: partdb cluster members [options]

        Options:
        """ + ClientOptions.USAGE;

    enum Query { STATUS, MEMBERS }

    record MemberView(String nodeId, String role, boolean isLeader) {}

    record MembershipView(Optional<String> leaderId, List<MemberView> members) {}

    ClusterCommand {
        Objects.requireNonNull(query, "query must not be null");
        Objects.requireNonNull(options, "options must not be null");
    }

    static AppCommand parse(Args args, Map<String, String> environment) {
        if (!args.hasNext()) {
            return new Help(USAGE);
        }
        return switch (args.next()) {
            case "status" -> parse(args, environment, Query.STATUS, STATUS_USAGE);
            case "members" -> parse(args, environment, Query.MEMBERS, MEMBERS_USAGE);
            case "help", "--help", "-h" -> new Help(USAGE);
            case String other -> new UsageError("unknown cluster command: " + other, USAGE);
        };
    }

    @Override
    public ExitCode run(PartDbClient client, PrintStream out, PrintStream err) {
        switch (query) {
            case STATUS -> {
                var status = client.cluster().status(options.endpoints().getFirst());
                print(out, status, text(status));
            }
            case MEMBERS -> {
                var membership = view(client.cluster().membership());
                print(out, membership, text(membership));
            }
        }
        return ExitCode.OK;
    }

    private static AppCommand parse(Args args, Map<String, String> environment, Query query, String usage) {
        var options = new ClientOptions.Parser(environment);
        try {
            while (args.hasNext()) {
                String arg = args.next();
                if (arg.equals("--help") || arg.equals("-h")) {
                    return new Help(usage);
                }
                if (!options.accept(arg, args)) {
                    throw new IllegalArgumentException((arg.startsWith("-") ? "unknown option: " : "unexpected argument: ") + arg);
                }
            }
            return new ClusterCommand(query, options.build());
        } catch (IllegalArgumentException e) {
            return new UsageError(e.getMessage(), usage);
        }
    }

    private static MembershipView view(Membership membership) {
        var members = membership.members().stream()
            .map(member -> new MemberView(member.nodeId(), role(member), membership.leaderId().map(member.nodeId()::equals).orElse(false)))
            .toList();
        return new MembershipView(membership.leaderId(), members);
    }

    private static String role(Member member) {
        return switch (member.role()) {
            case VOTER -> "voter";
            case LEARNER -> "learner";
        };
    }

    private static String text(NodeStatus status) {
        return """
            Node ID:        %s
            Role:           %s
            Term:           %d
            Leader:         %s
            Commit index:   %d
            Applied index:  %d
            Snapshot index: %d
            """.formatted(status.nodeId(), status.role(), status.term(), status.leaderId().orElse("unknown"),
            status.commitIndex(), status.appliedIndex(), status.snapshotIndex());
    }

    private static String text(MembershipView membership) {
        var text = new StringBuilder();
        for (var member : membership.members()) {
            text.append("%-20s %-8s%s%n".formatted(member.nodeId(), member.role(), member.isLeader() ? " leader" : ""));
        }
        return text.toString();
    }
}
