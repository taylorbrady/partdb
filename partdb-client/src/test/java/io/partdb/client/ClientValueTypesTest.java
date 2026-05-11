package io.partdb.client;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ClientValueTypesTest {

    @Test
    void keyValueHasValueSemantics() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var keyValue = new KeyValue(Bytes.copyOf(key), Bytes.copyOf(value), 7);

        key[0] = 'K';
        value[0] = 'V';

        assertEquals(Bytes.utf8("key"), keyValue.key());
        assertEquals(Bytes.utf8("value"), keyValue.value());
        assertEquals(new KeyValue(Bytes.utf8("key"), Bytes.utf8("value"), 7), keyValue);
    }

    @Test
    void writeOpsHaveValueSemantics() {
        byte[] key = "key".getBytes(StandardCharsets.UTF_8);
        byte[] value = "value".getBytes(StandardCharsets.UTF_8);

        var put = new WriteOp.Put(Bytes.copyOf(key), Bytes.copyOf(value));
        var delete = new WriteOp.Delete(Bytes.copyOf(key));

        key[0] = 'K';
        value[0] = 'V';

        assertEquals(Bytes.utf8("key"), put.key());
        assertEquals(Bytes.utf8("value"), put.value());
        assertEquals(Bytes.utf8("key"), delete.key());
        assertEquals(new WriteOp.Put(Bytes.utf8("key"), Bytes.utf8("value")), put);
        assertEquals(new WriteOp.Delete(Bytes.utf8("key")), delete);
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
