package io.partdb.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemtableLifecycleTest extends StorageEngineCoreTestSupport {

    @Test
    void freezeIsIdempotentAndRejectsLaterWrites() {
        MutableMemtable memtable = new MutableMemtable();
        StoredEntry.Value initial = new StoredEntry.Value(key("key"), value("value"), 1);

        assertEquals(MutableMemtable.WriteResult.APPLIED, memtable.put(initial));

        ImmutableMemtable firstFreeze = memtable.freeze();
        ImmutableMemtable secondFreeze = memtable.freeze();

        assertSame(firstFreeze, secondFreeze);
        assertEquals(MutableMemtable.WriteResult.FROZEN, memtable.put(new StoredEntry.Value(key("other"), value("next"), 2)));
        assertTrue(firstFreeze.get(key("key")).isPresent());
        assertTrue(firstFreeze.get(key("other")).isEmpty());
    }
}
