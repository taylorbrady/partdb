package io.partdb.node.lease;

import io.partdb.bytes.Bytes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseRegistryTest {

    @Test
    void attachedKeysUseValueSemantics() {
        LeaseRegistry registry = new LeaseRegistry();
        registry.grant(1, 1_000_000_000);

        registry.attachKey(1, Bytes.copyOf(new byte[]{1, 2, 3}));

        assertEquals(1, registry.attachedKeys(1).size());
        assertEquals(Bytes.copyOf(new byte[]{1, 2, 3}), registry.attachedKeys(1).get(0));

        registry.detachKey(1, Bytes.copyOf(new byte[]{1, 2, 3}));

        assertTrue(registry.attachedKeys(1).isEmpty());
    }
}
