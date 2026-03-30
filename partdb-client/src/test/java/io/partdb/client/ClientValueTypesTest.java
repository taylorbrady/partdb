package io.partdb.client;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class ClientValueTypesTest {

    @Test
    void keyValueDefensivelyCopiesArrays() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var keyValue = new KeyValue(key, value, 7);

        key[0] = 'K';
        value[0] = 'V';

        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), keyValue.key());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), keyValue.value());
        assertNotSame(key, keyValue.key());
        assertNotSame(value, keyValue.value());

        byte[] returnedKey = keyValue.key();
        byte[] returnedValue = keyValue.value();
        returnedKey[0] = 'X';
        returnedValue[0] = 'Y';

        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), keyValue.key());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), keyValue.value());
    }

    @Test
    void writeOpsDefensivelyCopyArrays() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var put = new WriteOp.Put(key, value, 5);
        var delete = new WriteOp.Delete(key);

        key[0] = 'K';
        value[0] = 'V';

        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), put.key());
        assertArrayEquals("value".getBytes(StandardCharsets.UTF_8), put.value());
        assertArrayEquals("key".getBytes(StandardCharsets.UTF_8), delete.key());
    }

    @Test
    void clusterMemberCarriesTypedEndpoint() {
        var endpoint = new ServerEndpoint("localhost", 8100);
        var member = new ClusterMember(
            "node-1",
            Optional.of(endpoint),
            ClusterMemberRole.VOTER,
            true,
            false
        );

        assertEquals(Optional.of(endpoint), member.raftEndpoint());
    }
}
