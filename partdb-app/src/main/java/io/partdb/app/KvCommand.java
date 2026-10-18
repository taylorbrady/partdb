package io.partdb.app;

import io.partdb.bytes.Bytes;
import io.partdb.client.Condition;
import io.partdb.client.KeyRange;
import io.partdb.client.KeyValue;
import io.partdb.client.Mutation;
import io.partdb.client.PartDbClient;
import io.partdb.client.ReadConsistency;
import io.partdb.client.ScanOptions;
import io.partdb.client.Txn;
import io.partdb.client.TxnResult;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Function;

record KvCommand(Operation operation, ClientOptions options) implements ClientCommand {

    static final String USAGE = """
        Usage: partdb kv <command>

        Commands:
          get       Read a key
          put       Write a key
          delete    Delete a key
          scan      Read a range of keys
        """;

    private static final String GET_USAGE = """
        Usage: partdb kv get <key> [options]

        Options:
              --local              Read local state without a linearizable read barrier
        """ + ClientOptions.USAGE;

    private static final String PUT_USAGE = """
        Usage: partdb kv put <key> <value> [options]

        Options:
              --if-missing         Write only if the key does not exist
              --if-revision <n>    Write only if the key was last modified at revision n
        """ + ClientOptions.USAGE;

    private static final String DELETE_USAGE = """
        Usage: partdb kv delete <key> [options]

        Options:
              --if-revision <n>    Delete only if the key was last modified at revision n
        """ + ClientOptions.USAGE;

    private static final String SCAN_USAGE = """
        Usage: partdb kv scan [<start> [<end>]] [options]

        Reads keys from start (inclusive) to end (exclusive).

        Options:
              --limit <n>          Return at most n keys (default: no limit)
              --local              Read local state without a linearizable read barrier
        """ + ClientOptions.USAGE;

    sealed interface Operation {
        record Get(Bytes key, ReadConsistency consistency) implements Operation {}

        record Put(Bytes key, Bytes value, List<Condition> conditions) implements Operation {
            public Put {
                conditions = List.copyOf(conditions);
            }
        }

        record Delete(Bytes key, List<Condition> conditions) implements Operation {
            public Delete {
                conditions = List.copyOf(conditions);
            }
        }

        record Scan(KeyRange range, ScanOptions options) implements Operation {}
    }

    record Entry(String key, String value, long revision) {
        static Entry of(KeyValue entry) {
            return new Entry(entry.key().utf8(), entry.value().utf8(), entry.revision());
        }
    }

    record Written(long revision) {}

    record Scanned(long readRevision, List<Entry> entries) {}

    private enum Flag { LOCAL, LIMIT, IF_MISSING, IF_REVISION }

    KvCommand {
        Objects.requireNonNull(operation, "operation must not be null");
        Objects.requireNonNull(options, "options must not be null");
    }

    static AppCommand parse(Args args, Map<String, String> environment) {
        if (!args.hasNext()) {
            return new Help(USAGE);
        }
        return switch (args.next()) {
            case "get" -> parse(args, environment, GET_USAGE, 1, 1, EnumSet.of(Flag.LOCAL), parsed ->
                new Operation.Get(Bytes.utf8(parsed.positional(0)), parsed.consistency));
            case "put" -> parse(args, environment, PUT_USAGE, 2, 2, EnumSet.of(Flag.IF_MISSING, Flag.IF_REVISION), parsed -> {
                var key = Bytes.utf8(parsed.positional(0));
                return new Operation.Put(key, Bytes.utf8(parsed.positional(1)), parsed.conditions(key));
            });
            case "delete" -> parse(args, environment, DELETE_USAGE, 1, 1, EnumSet.of(Flag.IF_REVISION), parsed -> {
                var key = Bytes.utf8(parsed.positional(0));
                return new Operation.Delete(key, parsed.conditions(key));
            });
            case "scan" -> parse(args, environment, SCAN_USAGE, 0, 2, EnumSet.of(Flag.LOCAL, Flag.LIMIT), parsed -> new Operation.Scan(
                new KeyRange(Bytes.utf8(parsed.positionals.isEmpty() ? "" : parsed.positional(0)),
                    parsed.positionals.size() < 2 ? Optional.empty() : Optional.of(Bytes.utf8(parsed.positional(1)))),
                parsed.scan()));
            case "help", "--help", "-h" -> new Help(USAGE);
            case String other -> new UsageError("unknown kv command: " + other, USAGE);
        };
    }

    @Override
    public ExitCode run(PartDbClient client, PrintStream out, PrintStream err) {
        return switch (operation) {
            case Operation.Get(var key, var consistency) -> {
                var entry = client.kv().get(key, consistency).value();
                if (entry.isEmpty()) {
                    err.println("Error: key not found: " + key.utf8());
                    yield ExitCode.NOT_FOUND;
                }
                var found = Entry.of(entry.get());
                print(out, found, found.value() + "\n");
                yield ExitCode.OK;
            }
            case Operation.Put(var key, var value, var conditions) ->
                committed(out, err, client.kv().transact(Txn.when(conditions, List.of(Mutation.put(key, value)))));
            case Operation.Delete(var key, var conditions) ->
                committed(out, err, client.kv().transact(Txn.when(conditions, List.of(Mutation.delete(key)))));
            case Operation.Scan(var range, var scanOptions) -> {
                try (var cursor = client.kv().scan(range, scanOptions)) {
                    if (options.output() == OutputFormat.JSON) {
                        var entries = new ArrayList<Entry>();
                        cursor.forEachRemaining(entry -> entries.add(Entry.of(entry)));
                        out.println(Json.write(new Scanned(cursor.readRevision(), entries)));
                    } else {
                        cursor.forEachRemaining(entry -> out.println(entry.key().utf8() + "=" + entry.value().utf8()));
                    }
                }
                yield ExitCode.OK;
            }
        };
    }

    private ExitCode committed(PrintStream out, PrintStream err, TxnResult result) {
        return switch (result) {
            case TxnResult.Committed(long revision) -> {
                print(out, new Written(revision), "OK revision=" + revision + "\n");
                yield ExitCode.OK;
            }
            case TxnResult.ConditionFailed() -> {
                err.println("Error: condition failed");
                yield ExitCode.CONDITION_FAILED;
            }
        };
    }

    private static final class Parsed {
        private final List<String> positionals = new ArrayList<>();
        private ReadConsistency consistency = ReadConsistency.LINEARIZABLE;
        private ScanOptions scanOptions = ScanOptions.DEFAULT;
        private boolean ifMissing;
        private OptionalLong ifRevision = OptionalLong.empty();

        private String positional(int index) {
            return positionals.get(index);
        }

        private List<Condition> conditions(Bytes key) {
            if (ifMissing && ifRevision.isPresent()) {
                throw new IllegalArgumentException("--if-missing and --if-revision cannot be combined");
            }
            if (ifMissing) {
                return List.of(Condition.missing(key));
            }
            return ifRevision.stream().mapToObj(revision -> (Condition) Condition.revisionEquals(key, revision)).toList();
        }

        private ScanOptions scan() {
            return scanOptions.withConsistency(consistency);
        }
    }

    private static AppCommand parse(
        Args args,
        Map<String, String> environment,
        String usage,
        int minArgs,
        int maxArgs,
        Set<Flag> flags,
        Function<Parsed, Operation> operation
    ) {
        var options = new ClientOptions.Parser(environment);
        var parsed = new Parsed();
        try {
            while (args.hasNext()) {
                String arg = args.next();
                switch (arg) {
                    case "--help", "-h" -> {
                        return new Help(usage);
                    }
                    case "--local" -> {
                        allow(flags, Flag.LOCAL, arg);
                        parsed.consistency = ReadConsistency.LOCAL;
                    }
                    case "--if-missing" -> {
                        allow(flags, Flag.IF_MISSING, arg);
                        parsed.ifMissing = true;
                    }
                    case "--if-revision" -> {
                        allow(flags, Flag.IF_REVISION, arg);
                        parsed.ifRevision = OptionalLong.of(args.positiveLong(arg));
                    }
                    case "--limit" -> {
                        allow(flags, Flag.LIMIT, arg);
                        parsed.scanOptions = parsed.scanOptions.withLimit(args.positiveInt(arg));
                    }
                    default -> {
                        if (!options.accept(arg, args)) {
                            if (arg.startsWith("-")) {
                                throw new IllegalArgumentException("unknown option: " + arg);
                            }
                            parsed.positionals.add(arg);
                        }
                    }
                }
            }
            int count = parsed.positionals.size();
            if (count < minArgs || count > maxArgs) {
                throw new IllegalArgumentException(minArgs == maxArgs
                    ? "expected " + minArgs + " argument(s), got " + count
                    : "expected " + minArgs + " to " + maxArgs + " arguments, got " + count);
            }
            return new KvCommand(operation.apply(parsed), options.build());
        } catch (IllegalArgumentException e) {
            return new UsageError(e.getMessage(), usage);
        }
    }

    private static void allow(Set<Flag> flags, Flag flag, String option) {
        if (!flags.contains(flag)) {
            throw new IllegalArgumentException("unknown option: " + option);
        }
    }
}
