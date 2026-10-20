package io.partdb.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import org.slf4j.event.KeyValuePair;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogEncoderTest {

    private static final Instant NOW = Instant.parse("2026-10-19T12:00:00.123Z");
    private static final String TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault()).format(NOW);

    @Test
    void writesTextWithQuotedValuesOnlyWhenNeeded() {
        var encoder = new LogEncoder();

        String line = encode(encoder, event(null));

        assertEquals(TIME + " INFO  RaftRuntime: Raft role changed nodeId=node1 term=3 reason=\"no \\\"leader\\\" yet\" empty=\"\"\n", line);
    }

    @Test
    void writesFlatJsonWithTypedValues() {
        var encoder = new LogEncoder();
        encoder.setFormat("json");

        String line = encode(encoder, event(null));

        assertEquals("{\"time\":\"" + TIME + "\",\"level\":\"INFO\",\"logger\":\"io.partdb.raft.runtime.RaftRuntime\",\"thread\":\"raft-node1\","
            + "\"msg\":\"Raft role changed\",\"nodeId\":\"node1\",\"term\":3,\"reason\":\"no \\\"leader\\\" yet\",\"empty\":\"\"}\n", line);
    }

    @Test
    void writesErrorsAsFieldsInJsonAndAsATraceInText() {
        var json = new LogEncoder();
        json.setFormat("json");
        var text = new LogEncoder();
        var error = new IllegalStateException("disk full");

        String jsonLine = encode(json, event(error));
        String textLines = encode(text, event(error));

        assertTrue(jsonLine.contains("\"error\":\"java.lang.IllegalStateException: disk full\",\"stack\":\"java.lang.IllegalStateException: disk full\\n"), jsonLine);
        assertTrue(jsonLine.endsWith("}\n") && jsonLine.indexOf('\n') == jsonLine.length() - 1, jsonLine);
        assertTrue(textLines.contains("\njava.lang.IllegalStateException: disk full\n\tat "), textLines);
    }

    private static LoggingEvent event(Throwable error) {
        var logger = new LoggerContext().getLogger("io.partdb.raft.runtime.RaftRuntime");
        var event = new LoggingEvent(LogEncoderTest.class.getName(), logger, Level.INFO, "Raft role changed", error, null);
        event.setInstant(NOW);
        event.setThreadName("raft-node1");
        event.addKeyValuePair(new KeyValuePair("nodeId", "node1"));
        event.addKeyValuePair(new KeyValuePair("term", 3L));
        event.addKeyValuePair(new KeyValuePair("reason", "no \"leader\" yet"));
        event.addKeyValuePair(new KeyValuePair("empty", ""));
        return event;
    }

    private static String encode(LogEncoder encoder, LoggingEvent event) {
        return new String(encoder.encode(event), StandardCharsets.UTF_8);
    }
}
