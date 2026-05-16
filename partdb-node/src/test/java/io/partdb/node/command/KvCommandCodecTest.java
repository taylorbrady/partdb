package io.partdb.node.command;

import io.partdb.bytes.Bytes;
import io.partdb.node.kv.Condition;
import io.partdb.node.kv.Transaction;
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

    @Test
    void roundTripsCompareAndWriteCommand() {
        var command = new KvCommand.CompareAndWrite(
            Transaction.builder()
                .require(Condition.exists(Bytes.utf8("guard-1")))
                .require(Condition.missing(Bytes.utf8("guard-2")))
                .require(Condition.valueEquals(Bytes.utf8("guard-3"), Bytes.utf8("expected")))
                .require(Condition.revisionEquals(Bytes.utf8("guard-4"), 12))
                .put(Bytes.utf8("key-1"), Bytes.utf8("value-1"))
                .delete(Bytes.utf8("key-2"))
                .build()
        );

        assertEquals(command, KvCommandCodec.decode(KvCommandCodec.encode(command)));
    }
}
