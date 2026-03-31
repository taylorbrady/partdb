package io.partdb.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemtableLifecycleTest extends StoreRuntimeTestSupport {

    @Test
    void freezeIsIdempotentAndRejectsLaterWrites() {
        MutableMemtable memtable = new MutableMemtable();
        Mutation.Put initial = new Mutation.Put(key("key"), value("value"), 1);

        assertEquals(MutableMemtable.WriteResult.APPLIED, memtable.put(initial));

        ImmutableMemtable firstFreeze = memtable.freeze();
        ImmutableMemtable secondFreeze = memtable.freeze();

        assertSame(firstFreeze, secondFreeze);
        assertEquals(MutableMemtable.WriteResult.FROZEN, memtable.put(new Mutation.Put(key("other"), value("next"), 2)));
        assertTrue(firstFreeze.get(key("key")).isPresent());
        assertTrue(firstFreeze.get(key("other")).isEmpty());
    }
}
