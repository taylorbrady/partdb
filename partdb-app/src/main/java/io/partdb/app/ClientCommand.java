package io.partdb.app;

import io.partdb.bytes.Bytes;
import io.partdb.client.KeyRange;
import io.partdb.client.KeyValue;
import io.partdb.client.Member;
import io.partdb.client.MemberRole;
import io.partdb.client.Members;
import io.partdb.client.NodeStatus;
import io.partdb.client.PartDbClient;
import io.partdb.client.PartDbException;
import io.partdb.client.ReadConsistency;
import io.partdb.client.ScanOptions;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

record ClientCommand(Action action, ClientOptions options) implements AppCommand {

    sealed interface Action {
        record Get(String key, ReadConsistency consistency) implements Action {}

        record Put(String key, String value) implements Action {}

        record Delete(String key) implements Action {}

        record Scan(String start, Optional<String> end, int limit, ReadConsistency consistency) implements Action {}

        record Status() implements Action {}

        record Members() implements Action {}
    }

    ClientCommand {
        Objects.requireNonNull(action, "action must not be null");
        Objects.requireNonNull(options, "options must not be null");
    }

    @Override
    public int execute(PrintStream out, PrintStream err) {
        try (var client = PartDbClient.builder().endpoints(options.endpoints()).requestTimeout(options.timeout()).build()) {
            return run(client, out, err);
        } catch (PartDbException e) {
            err.println("Error: " + describe(e));
            return FAILED;
        }
    }

    private int run(PartDbClient client, PrintStream out, PrintStream err) {
        boolean json = options.output() == OutputFormat.JSON;
        switch (action) {
            case Action.Get(var key, var consistency) -> {
                var entry = client.kv().get(Bytes.utf8(key), consistency);
                if (entry.isEmpty()) {
                    err.println("Error: key not found: " + key);
                    return NOT_FOUND;
                }
                out.println(json ? Json.write(entry(entry.get())) : entry.get().value().utf8());
            }
            case Action.Put(var key, var value) -> written(out, json, client.kv().put(Bytes.utf8(key), Bytes.utf8(value)));
            case Action.Delete(var key) -> written(out, json, client.kv().delete(Bytes.utf8(key)));
            case Action.Scan(var start, var end, int limit, var consistency) -> {
                var range = new KeyRange(Bytes.utf8(start), end.map(Bytes::utf8));
                try (var cursor = client.kv().scan(range, new ScanOptions(limit, consistency))) {
                    var entries = new ArrayList<Map<String, Object>>();
                    while (cursor.hasNext()) {
                        var entry = cursor.next();
                        if (json) {
                            entries.add(entry(entry));
                        } else {
                            out.println(entry.key().utf8() + "=" + entry.value().utf8());
                        }
                    }
                    if (json) {
                        out.println(Json.write(map("readRevision", cursor.readRevision(), "entries", entries)));
                    }
                }
            }
            case Action.Status() -> {
                var status = client.cluster().status(options.endpoints().getFirst());
                out.print(json ? Json.write(status(status)) + "\n" : text(status));
            }
            case Action.Members() -> {
                var members = client.cluster().members();
                out.print(json ? Json.write(members(members)) + "\n" : text(members));
            }
        }
        return OK;
    }

    private static void written(PrintStream out, boolean json, long revision) {
        out.println(json ? Json.write(map("revision", revision)) : "OK revision=" + revision);
    }

    private static Map<String, Object> entry(KeyValue entry) {
        return map("key", entry.key().utf8(), "value", entry.value().utf8(), "revision", entry.revision());
    }

    private static Map<String, Object> status(NodeStatus status) {
        return map(
            "nodeId", status.nodeId(),
            "role", status.role().name(),
            "term", status.term(),
            "leaderId", status.leaderId().orElse(null),
            "commitIndex", status.commitIndex(),
            "appliedIndex", status.appliedIndex(),
            "snapshotIndex", status.snapshotIndex()
        );
    }

    private static Map<String, Object> members(Members members) {
        var list = members.members().stream()
            .map(member -> (Object) map(
                "nodeId", member.nodeId(),
                "role", role(member.role()),
                "isLeader", members.leaderId().map(member.nodeId()::equals).orElse(false)
            ))
            .toList();
        return map("leaderId", members.leaderId().orElse(null), "members", list);
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

    private static String text(Members members) {
        var text = new StringBuilder();
        for (Member member : members.members()) {
            boolean leader = members.leaderId().map(member.nodeId()::equals).orElse(false);
            text.append("%-20s %-8s%s%n".formatted(member.nodeId(), role(member.role()), leader ? " leader" : ""));
        }
        return text.toString();
    }

    private static String role(MemberRole role) {
        return switch (role) {
            case VOTER -> "voter";
            case LEARNER -> "learner";
        };
    }

    private static String describe(PartDbException error) {
        String kind = switch (error) {
            case PartDbException.Unavailable _ -> "unavailable";
            case PartDbException.Timeout _ -> "timed out";
            case PartDbException.InvalidRequest _ -> "invalid request";
            case PartDbException.OutcomeUnknown _ -> "outcome unknown";
            case PartDbException.Internal _ -> "server error";
            case PartDbException.Cancelled _ -> "cancelled";
            case PartDbException.Closed _ -> "client closed";
        };
        return kind + ": " + error.getMessage();
    }

    private static Map<String, Object> map(Object... pairs) {
        var map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put((String) pairs[i], pairs[i + 1]);
        }
        return map;
    }
}
