package io.partdb.raft;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PublicValueTypesTest {

    @Test
    void logEntryDataHasValueSemantics() {
        byte[] payload = "entry".getBytes(StandardCharsets.UTF_8);
        var entry = new LogEntry.Data(1, 1, Bytes.copyOf(payload));

        payload[0] = 'E';
        assertEquals(Bytes.utf8("entry"), entry.data());
        assertEquals(new LogEntry.Data(1, 1, Bytes.utf8("entry")), entry);
    }

    @Test
    void snapshotHasValueSemantics() {
        byte[] data = "snapshot".getBytes(StandardCharsets.UTF_8);
        var snapshot = new RaftSnapshot(3, 2, RaftMembership.ofVoters("n1"), Bytes.copyOf(data));

        data[0] = 'S';
        assertEquals(Bytes.utf8("snapshot"), snapshot.data());
        assertEquals(new RaftSnapshot(3, 2, RaftMembership.ofVoters("n1"), Bytes.utf8("snapshot")), snapshot);
    }

    @Test
    void readIndexMessageHasValueSemantics() {
        byte[] context = "ctx".getBytes(StandardCharsets.UTF_8);
        var message = new RaftMessage.ReadIndex(4, Bytes.copyOf(context));

        context[0] = 'C';
        assertEquals(Bytes.utf8("ctx"), message.context());
        assertEquals(new RaftMessage.ReadIndex(4, Bytes.utf8("ctx")), message);
    }

    @Test
    void readyUsesBytesValues() {
        byte[] data = "apply".getBytes(StandardCharsets.UTF_8);
        var applyEntry = new RaftReady.ApplyEntry(2, 1, Bytes.copyOf(data));
        var ready = new RaftReady(
            RaftReady.Persistence.EMPTY,
            List.of(),
            new RaftReady.Application(List.of(applyEntry), List.of(), List.of()),
            Optional.empty()
        );

        data[0] = 'A';
        assertEquals(Bytes.utf8("apply"), ready.application().entries().getFirst().data());
        assertEquals(new RaftReady.ApplyEntry(2, 1, Bytes.utf8("apply")), ready.application().entries().getFirst());
    }
}
