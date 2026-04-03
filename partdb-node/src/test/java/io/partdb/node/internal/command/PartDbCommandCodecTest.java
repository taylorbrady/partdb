package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartDbCommandCodecTest {

    @Test
    void roundTripsPutCommand() {
        var command = new PartDbCommand.Put(Bytes.utf8("key"), Bytes.utf8("value"), 42);

        assertEquals(command, PartDbCommandCodec.decode(PartDbCommandCodec.encode(command)));
    }

    @Test
    void roundTripsLeaseCommands() {
        assertEquals(
            new PartDbCommand.GrantLease(123),
            PartDbCommandCodec.decode(PartDbCommandCodec.encode(new PartDbCommand.GrantLease(123)))
        );
        assertEquals(
            new PartDbCommand.RevokeLease(7),
            PartDbCommandCodec.decode(PartDbCommandCodec.encode(new PartDbCommand.RevokeLease(7)))
        );
        assertEquals(
            new PartDbCommand.KeepAliveLease(7),
            PartDbCommandCodec.decode(PartDbCommandCodec.encode(new PartDbCommand.KeepAliveLease(7)))
        );
        assertEquals(
            new PartDbCommand.ExpireLease(7),
            PartDbCommandCodec.decode(PartDbCommandCodec.encode(new PartDbCommand.ExpireLease(7)))
        );
    }

    @Test
    void roundTripsBatchWriteCommand() {
        var command = new PartDbCommand.BatchWrite(
            WriteBatch.builder()
                .put(Bytes.utf8("key-1"), Bytes.utf8("value-1"))
                .delete(Bytes.utf8("key-2"))
                .build()
        );

        assertEquals(command, PartDbCommandCodec.decode(PartDbCommandCodec.encode(command)));
    }
}
