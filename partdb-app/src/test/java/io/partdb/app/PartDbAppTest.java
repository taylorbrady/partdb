package io.partdb.app;

import io.partdb.bytes.Bytes;
import io.partdb.client.Condition;
import io.partdb.client.KeyRange;
import io.partdb.client.ReadConsistency;
import io.partdb.client.ScanOptions;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbAppTest {

    private static AppCommand parse(String... args) {
        return PartDbApp.parse(args, Map.of());
    }

    @Test
    void printsUsageWithoutArguments() {
        var out = new ByteArrayOutputStream();

        var exit = parse().execute(new PrintStream(out, true, StandardCharsets.UTF_8), System.err);

        assertEquals(ExitCode.OK, exit);
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("Usage: partdb <namespace> <command> [options]"));
    }

    @Test
    void parsesGetWithEndpointsAndConsistency() {
        var command = assertInstanceOf(KvCommand.class, parse("kv", "get", "hello", "--endpoints", "[::1]:8101,localhost:9101", "--local"));

        assertEquals(new KvCommand.Operation.Get(Bytes.utf8("hello"), ReadConsistency.LOCAL), command.operation());
        assertEquals(List.of("[::1]:8101", "localhost:9101"), command.options().endpoints().stream().map(Object::toString).toList());
    }

    @Test
    void readsEndpointsFromTheEnvironment() {
        var command = assertInstanceOf(ClusterCommand.class,
            PartDbApp.parse(new String[] {"cluster", "status"}, Map.of(ClientOptions.ENDPOINTS_VARIABLE, "a:1,b:2")));

        assertEquals(ClusterCommand.Query.STATUS, command.query());
        assertEquals(2, command.options().endpoints().size());
    }

    @Test
    void parsesScanBoundsAndLimit() {
        var command = assertInstanceOf(KvCommand.class, parse("kv", "scan", "a", "m", "--limit", "5", "--output", "json"));

        assertEquals(new KvCommand.Operation.Scan(KeyRange.between(Bytes.utf8("a"), Bytes.utf8("m")), ScanOptions.DEFAULT.withLimit(5)), command.operation());
        assertEquals(OutputFormat.JSON, command.options().output());
    }

    @Test
    void reportsUsageErrorsWithExitCodeTwo() {
        var err = new ByteArrayOutputStream();

        var exit = parse("kv", "put", "only-key").execute(System.out, new PrintStream(err, true, StandardCharsets.UTF_8));

        assertEquals(ExitCode.USAGE, exit);
        assertTrue(err.toString(StandardCharsets.UTF_8).startsWith("Error: expected 2 argument(s), got 1"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "get", "key", "--bogus"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "put", "key", "value", "--local"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "scan", "z", "a"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "scan", "--limit", "0"));
        assertInstanceOf(AppCommand.UsageError.class, parse("cluster", "status", "extra"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "put", "k", "v", "--if-missing", "--if-revision", "3"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "delete", "k", "--if-missing"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "put", "k", "v", "--if-revision", "0"));
        assertInstanceOf(AppCommand.UsageError.class, parse("server", "start", "--node-id", "n1"));
    }

    @Test
    void parsesConditionalWrites() {
        var put = assertInstanceOf(KvCommand.class, parse("kv", "put", "k", "v", "--if-revision", "7"));
        var delete = assertInstanceOf(KvCommand.class, parse("kv", "delete", "--if-revision", "9", "k"));
        var create = assertInstanceOf(KvCommand.class, parse("kv", "put", "k", "v", "--if-missing"));

        assertEquals(new KvCommand.Operation.Put(Bytes.utf8("k"), Bytes.utf8("v"), List.of(Condition.revisionEquals(Bytes.utf8("k"), 7))), put.operation());
        assertEquals(new KvCommand.Operation.Delete(Bytes.utf8("k"), List.of(Condition.revisionEquals(Bytes.utf8("k"), 9))), delete.operation());
        assertEquals(new KvCommand.Operation.Put(Bytes.utf8("k"), Bytes.utf8("v"), List.of(Condition.missing(Bytes.utf8("k")))), create.operation());
    }

    @Test
    void parsesServerStartWithPeers() {
        var command = assertInstanceOf(StartCommand.class, parse(
            "server", "start", "--node-id", "node1", "--data-dir", "/tmp/partdb",
            "--peer", "node1=localhost:8100", "--peer", "node2=localhost:8200", "--client-port", "8101", "--log-retention-entries", "50"));

        assertEquals(Optional.of(8100), command.config().raftPort());
        assertEquals(50, command.config().retention().entries());
        assertEquals(OutputFormat.TEXT, command.logFormat());
        assertEquals(Level.INFO, command.logLevel());
    }

    @Test
    void parsesServerLogOptions() {
        var command = assertInstanceOf(StartCommand.class, parse(
            "server", "start", "--node-id", "node1", "--data-dir", "/tmp/partdb", "--log-format", "json", "--log-level", "debug"));

        assertEquals(OutputFormat.JSON, command.logFormat());
        assertEquals(Level.DEBUG, command.logLevel());
        assertInstanceOf(AppCommand.UsageError.class, parse("server", "start", "-n", "n1", "-d", "/tmp/p", "--log-format", "xml"));
        assertInstanceOf(AppCommand.UsageError.class, parse("server", "start", "-n", "n1", "-d", "/tmp/p", "--log-level", "loud"));
    }

    record Sample(String text, List<Object> values, Optional<String> missing, ExitCode exit) {}

    @Test
    void writesRecordsAsEscapedJson() {
        var sample = new Sample("x\"y\n", Arrays.asList(1, true, null), Optional.empty(), ExitCode.OK);

        assertEquals("{\"text\":\"x\\\"y\\n\",\"values\":[1,true,null],\"missing\":null,\"exit\":\"OK\"}", Json.write(sample));
    }
}
