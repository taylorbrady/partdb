package io.partdb.app;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PartDbAppTest {

    @Test
    void runVersionPrintsVersion() {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);

        int exitCode = PartDbApp.run(new String[] {"version"}, out, err);

        assertEquals(0, exitCode);
        assertEquals("partdb 0.1.0-SNAPSHOT\n", outBytes.toString(StandardCharsets.UTF_8));
    }

    @Test
    void runWithoutArgsPrintsUsage() {
        ByteArrayOutputStream outBytes = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(outBytes, true, StandardCharsets.UTF_8);
        PrintStream err = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);

        int exitCode = PartDbApp.run(new String[0], out, err);

        assertEquals(0, exitCode);
        assertTrue(outBytes.toString(StandardCharsets.UTF_8).contains("Usage: partdb <command> [options]"));
    }

    @Test
    void parseGetBuildsImmutableCommandRecord() {
        AppCommand command = PartDbApp.parse(new String[] {"get", "hello", "--endpoint", "[::1]:8101"});

        var get = assertInstanceOf(GetCommand.class, command);
        assertEquals("hello", get.key());
        assertEquals("::1", get.endpoint().host());
        assertEquals(8101, get.endpoint().port());
    }
}
