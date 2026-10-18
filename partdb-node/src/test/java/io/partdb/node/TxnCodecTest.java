package io.partdb.node;

import io.partdb.bytes.Bytes;
import io.partdb.storage.Mutation;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TxnCodecTest {

    private static Bytes bytes(String value) {
        return Bytes.utf8(value);
    }

    @Test
    void roundTripsEveryConditionAndMutation() {
        var txn = Txn.when(
            List.of(
                Condition.exists(bytes("a")),
                Condition.missing(bytes("b")),
                Condition.valueEquals(bytes("c"), bytes("value")),
                Condition.revisionEquals(bytes("d"), 42)
            ),
            List.of(Mutation.put(bytes("e"), bytes("v")), Mutation.delete(bytes("f")), Mutation.put(bytes("g"), Bytes.EMPTY))
        );

        assertEquals(txn, TxnCodec.decode(TxnCodec.encode(txn)));
    }

    @Test
    void rejectsEveryTruncation() {
        byte[] encoded = TxnCodec.encode(Txn.when(
            List.of(Condition.valueEquals(bytes("key"), bytes("value"))),
            List.of(Mutation.put(bytes("key"), bytes("value")))
        )).toByteArray();

        for (int length = 0; length < encoded.length; length++) {
            var truncated = Bytes.copyOf(Arrays.copyOf(encoded, length));
            assertThrows(TxnCodec.CorruptCommand.class, () -> TxnCodec.decode(truncated));
        }
    }

    @Test
    void rejectsTrailingBytesUnknownVersionsAndTags() {
        byte[] encoded = TxnCodec.encode(Txn.of(List.of(Mutation.delete(bytes("key"))))).toByteArray();

        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        assertThrows(TxnCodec.CorruptCommand.class, () -> TxnCodec.decode(Bytes.copyOf(trailing)));

        byte[] version = encoded.clone();
        version[0] = 9;
        assertThrows(TxnCodec.CorruptCommand.class, () -> TxnCodec.decode(Bytes.copyOf(version)));

        byte[] tag = encoded.clone();
        tag[1 + 4 + 4] = 7;
        assertThrows(TxnCodec.CorruptCommand.class, () -> TxnCodec.decode(Bytes.copyOf(tag)));
    }

    @Test
    void randomBytesNeverEscapeAsAnythingButCorruptCommand() {
        var random = new SplittableRandom(7);
        for (int i = 0; i < 10_000; i++) {
            byte[] noise = new byte[random.nextInt(64)];
            random.nextBytes(noise);
            try {
                TxnCodec.decode(Bytes.copyOf(noise));
            } catch (TxnCodec.CorruptCommand _) {
            }
        }
    }
}
