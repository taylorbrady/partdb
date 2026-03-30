package io.partdb.raft;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class PublicValueTypesTest {

    @Test
    void logEntryDataDefensivelyCopiesPayload() {
        byte[] payload = "entry".getBytes(StandardCharsets.UTF_8);
        var entry = new LogEntry.Data(1, 1, payload);

        payload[0] = 'E';
        assertArrayEquals("entry".getBytes(StandardCharsets.UTF_8), entry.data());

        byte[] returned = entry.data();
        returned[0] = 'X';
        assertArrayEquals("entry".getBytes(StandardCharsets.UTF_8), entry.data());
        assertNotSame(payload, entry.data());
    }

    @Test
    void snapshotDefensivelyCopiesPayload() {
        byte[] data = "snapshot".getBytes(StandardCharsets.UTF_8);
        var snapshot = new Snapshot(3, 2, Membership.ofVoters("n1"), data);

        data[0] = 'S';
        assertArrayEquals("snapshot".getBytes(StandardCharsets.UTF_8), snapshot.data());

        byte[] returned = snapshot.data();
        returned[0] = 'X';
        assertArrayEquals("snapshot".getBytes(StandardCharsets.UTF_8), snapshot.data());
    }

    @Test
    void readIndexMessageDefensivelyCopiesContext() {
        byte[] context = "ctx".getBytes(StandardCharsets.UTF_8);
        var message = new RaftMessage.ReadIndex(4, context);

        context[0] = 'C';
        assertArrayEquals("ctx".getBytes(StandardCharsets.UTF_8), message.context());

        byte[] returned = message.context();
        returned[0] = 'X';
        assertArrayEquals("ctx".getBytes(StandardCharsets.UTF_8), message.context());
    }

    @Test
    void readyDefensivelyCopiesApplyPayloadsAndLists() {
        byte[] data = "apply".getBytes(StandardCharsets.UTF_8);
        var applyEntry = new Ready.ApplyEntry(2, 1, data);
        var ready = new Ready(
            Ready.Persist.EMPTY,
            List.of(),
            new Ready.Apply(List.of(applyEntry), List.of(), List.of()),
            null
        );

        data[0] = 'A';
        assertArrayEquals("apply".getBytes(StandardCharsets.UTF_8), ready.apply().entries().getFirst().data());

        byte[] returned = ready.apply().entries().getFirst().data();
        returned[0] = 'X';
        assertArrayEquals("apply".getBytes(StandardCharsets.UTF_8), ready.apply().entries().getFirst().data());
    }
}
