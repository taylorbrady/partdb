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
        var entry = new RaftLogEntry.Data(1, 1, Bytes.copyOf(payload));

        payload[0] = 'E';
        assertEquals(Bytes.utf8("entry"), entry.data());
        assertEquals(new RaftLogEntry.Data(1, 1, Bytes.utf8("entry")), entry);
    }

    @Test
    void snapshotHasValueSemantics() {
        byte[] data = "snapshot".getBytes(StandardCharsets.UTF_8);
        var snapshot = new RaftSnapshot(3, 2, RaftMembership.voters("n1"), Bytes.copyOf(data));

        data[0] = 'S';
        assertEquals(Bytes.utf8("snapshot"), snapshot.data());
        assertEquals(new RaftSnapshot(3, 2, RaftMembership.voters("n1"), Bytes.utf8("snapshot")), snapshot);
    }

    @Test
    void readIndexMessageHasValueSemantics() {
        byte[] context = "ctx".getBytes(StandardCharsets.UTF_8);
        var message = new RaftMessage.ReadRequested(4, Bytes.copyOf(context));

        context[0] = 'C';
        assertEquals(Bytes.utf8("ctx"), message.context());
        assertEquals(new RaftMessage.ReadRequested(4, Bytes.utf8("ctx")), message);
    }

    @Test
    void effectsUseBytesValues() {
        byte[] data = "apply".getBytes(StandardCharsets.UTF_8);
        var applyEntry = new RaftEffects.ApplyEntry(2, 1, Bytes.copyOf(data));
        var effects = new RaftEffects(
            RaftEffects.Persistence.EMPTY,
            List.of(),
            new RaftEffects.Application(List.of(applyEntry), List.of(), List.of(), applyEntry.index()),
            Optional.empty()
        );

        data[0] = 'A';
        assertEquals(Bytes.utf8("apply"), effects.application().entries().getFirst().data());
        assertEquals(new RaftEffects.ApplyEntry(2, 1, Bytes.utf8("apply")), effects.application().entries().getFirst());
    }
}
