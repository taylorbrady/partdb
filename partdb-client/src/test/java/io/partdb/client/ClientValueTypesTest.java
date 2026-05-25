package io.partdb.client;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
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
    void clusterMemberCarriesClusterIdentity() {
        var member = new ClusterMember(
            "node-1",
            ClusterMemberRole.VOTER,
            true,
            false
        );

        assertEquals("node-1", member.nodeId());
        assertEquals(ClusterMemberRole.VOTER, member.role());
    }
}
