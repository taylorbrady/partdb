package io.partdb.app;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static java.util.Map.entry;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogFormatterTest {

    private static final Instant NOW = Instant.parse("2026-10-19T12:00:00.123Z");
    private static final String TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault()).format(NOW);

    @Test
    void writesTextFieldsQuotingOnlyWhenNeeded() {
        var text = new LogFormatter(OutputFormat.TEXT, "node1");

        String line = text.format(record(Level.INFO, "Raft role changed",
            entry("term", 3L), entry("reason", "no \"leader\" yet"), entry("leader", Optional.empty()), entry("took", Duration.ofMillis(87))));

        assertEquals(TIME + " INFO  RaftRuntime: Raft role changed nodeId=node1 term=3 reason=\"no \\\"leader\\\" yet\" took=87ms\n", line);
    }

    @Test
    void writesFlatJsonWithTypedValues() {
        var json = new LogFormatter(OutputFormat.JSON, "node1");

        String line = json.format(record(Level.WARNING, "Slow raft log sync",
            entry("entries", 12), entry("duration", Duration.ofMillis(1500)), entry("leader", Optional.of("node2")), entry("ok", true)));

        assertEquals("{\"time\":\"" + TIME + "\",\"level\":\"WARN\",\"logger\":\"io.partdb.raft.runtime.RaftRuntime\","
            + "\"thread\":\"" + Thread.currentThread().getName() + "\",\"nodeId\":\"node1\",\"msg\":\"Slow raft log sync\","
            + "\"entries\":12,\"duration\":1.5,\"leader\":\"node2\",\"ok\":true}\n", line);
    }

    @Test
    void takesTheErrorFromAThrowableParameter() {
        var error = new IllegalStateException("disk full");

        String json = new LogFormatter(OutputFormat.JSON, "node1").format(record(Level.SEVERE, "Compaction failed", entry("sourceLevel", 0), error));
        String text = new LogFormatter(OutputFormat.TEXT, "node1").format(record(Level.SEVERE, "Compaction failed", Optional.of(error)));

        assertTrue(json.contains("\"level\":\"ERROR\""), json);
        assertTrue(json.contains("\"sourceLevel\":0,\"error\":\"java.lang.IllegalStateException: disk full\",\"stack\":\"java.lang.IllegalStateException: disk full\\n\\tat "), json);
        assertEquals(json.length() - 1, json.indexOf('\n'));
        assertTrue(text.contains(": Compaction failed nodeId=node1\njava.lang.IllegalStateException: disk full\n\tat "), text);
    }

    @Test
    void formatsLibraryMessagesThatUsePlaceholders() {
        var record = record(Level.FINE, "Channel {0} closed", "c-1");
        record.setLoggerName("io.grpc.internal.ManagedChannelImpl");

        String line = new LogFormatter(OutputFormat.TEXT, "node1").format(record);

        assertEquals(TIME + " DEBUG ManagedChannelImpl: Channel c-1 closed nodeId=node1\n", line);
    }

    private static LogRecord record(Level level, String message, Object... parameters) {
        var record = new LogRecord(level, message);
        record.setLoggerName("io.partdb.raft.runtime.RaftRuntime");
        record.setInstant(NOW);
        record.setParameters(parameters);
        return record;
    }
}
