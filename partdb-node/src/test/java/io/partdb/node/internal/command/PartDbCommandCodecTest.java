package io.partdb.node.internal.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PartDbCommandCodecTest {

    @Test
    void roundTripsPutCommand() {
        var command = new PartDbCommand.Put(Bytes.utf8("key"), Bytes.utf8("value"));

        assertEquals(command, PartDbCommandCodec.decode(PartDbCommandCodec.encode(command)));
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
