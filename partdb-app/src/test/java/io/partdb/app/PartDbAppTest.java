package io.partdb.app;

import io.partdb.client.ReadConsistency;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

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

        int exitCode = parse().execute(new PrintStream(out, true, StandardCharsets.UTF_8), System.err);

        assertEquals(AppCommand.OK, exitCode);
        assertTrue(out.toString(StandardCharsets.UTF_8).contains("Usage: partdb <namespace> <command> [options]"));
    }

    @Test
    void parsesGetWithEndpointsAndConsistency() {
        var command = assertInstanceOf(ClientCommand.class, parse("kv", "get", "hello", "--endpoints", "[::1]:8101,localhost:9101", "--local"));

        assertEquals(new ClientCommand.Action.Get("hello", ReadConsistency.LOCAL), command.action());
        assertEquals(List.of("[::1]:8101", "localhost:9101"), command.options().endpoints().stream().map(Object::toString).toList());
    }

    @Test
    void readsEndpointsFromTheEnvironment() {
        var command = assertInstanceOf(ClientCommand.class,
            PartDbApp.parse(new String[] {"cluster", "status"}, Map.of(ClientOptions.ENDPOINTS_VARIABLE, "a:1,b:2")));

        assertEquals(2, command.options().endpoints().size());
    }

    @Test
    void parsesScanBoundsAndLimit() {
        var command = assertInstanceOf(ClientCommand.class, parse("kv", "scan", "a", "m", "--limit", "5", "--output", "json"));

        assertEquals(new ClientCommand.Action.Scan("a", Optional.of("m"), OptionalInt.of(5), ReadConsistency.LINEARIZABLE), command.action());
        assertEquals(OutputFormat.JSON, command.options().output());
    }

    @Test
    void reportsUsageErrorsWithExitCodeTwo() {
        var err = new ByteArrayOutputStream();

        int exitCode = parse("kv", "put", "only-key").execute(System.out, new PrintStream(err, true, StandardCharsets.UTF_8));

        assertEquals(AppCommand.USAGE, exitCode);
        assertTrue(err.toString(StandardCharsets.UTF_8).startsWith("Error: expected 2 argument(s), got 1"));
        assertInstanceOf(AppCommand.UsageError.class, parse("kv", "get", "key", "--bogus"));
        assertInstanceOf(AppCommand.UsageError.class, parse("server", "start", "--node-id", "n1"));
    }

    @Test
    void parsesServerStartWithPeers() {
        var command = assertInstanceOf(StartCommand.class, parse(
            "server", "start", "--node-id", "node1", "--data-dir", "/tmp/partdb",
            "--peer", "node1=localhost:8100", "--peer", "node2=localhost:8200", "--client-port", "8101", "--snapshot-entries", "50"));

        assertEquals(Optional.of(8100), command.config().raftPort());
        assertEquals(50, command.config().node().raft().snapshots().logEntries());
    }

    @Test
    void writesEscapedJson() {
        var value = new LinkedHashMap<String, Object>();
        value.put("a", "x\"y\n");
        value.put("b", Arrays.asList(1, true, null));

        assertEquals("{\"a\":\"x\\\"y\\n\",\"b\":[1,true,null]}", Json.write(value));
    }
}
