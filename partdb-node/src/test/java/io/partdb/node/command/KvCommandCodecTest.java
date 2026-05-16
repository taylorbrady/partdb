package io.partdb.node.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.WriteBatch;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class KvCommandCodecTest {

    @Test
    void roundTripsPutCommand() {
        var command = new KvCommand.Put(Bytes.utf8("key"), Bytes.utf8("value"));

        assertEquals(command, KvCommandCodec.decode(KvCommandCodec.encode(command)));
    }

    @Test
    void roundTripsBatchWriteCommand() {
        var command = new KvCommand.BatchWrite(
            WriteBatch.builder()
                .put(Bytes.utf8("key-1"), Bytes.utf8("value-1"))
                .delete(Bytes.utf8("key-2"))
                .build()
        );

        assertEquals(command, KvCommandCodec.decode(KvCommandCodec.encode(command)));
    }
}
