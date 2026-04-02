package io.partdb.storage;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MemtableLifecycleTest extends StorageEngineInternalTestSupport {

    @Test
    void freezeIsIdempotentAndRejectsLaterWrites() {
        MutableMemtable memtable = new MutableMemtable();
        StoredEntry.Value initial = new StoredEntry.Value(key("key"), value("value"), 1);

        assertEquals(MutableMemtable.WriteResult.APPLIED, memtable.put(initial));

        ImmutableMemtable firstFreeze = memtable.freeze();
        ImmutableMemtable secondFreeze = memtable.freeze();

        assertSame(firstFreeze, secondFreeze);
        assertEquals(MutableMemtable.WriteResult.FROZEN, memtable.put(new StoredEntry.Value(key("other"), value("next"), 2)));
        assertTrue(firstFreeze.get(key("key"), Long.MAX_VALUE).isPresent());
        assertTrue(firstFreeze.get(key("other"), Long.MAX_VALUE).isEmpty());
    }

    @Test
    void lookupHonorsSnapshotRevisionAgainstLiveMemtable() {
        MutableMemtable memtable = new MutableMemtable();
        memtable.put(new StoredEntry.Value(key("key"), value("older"), 1));
        long snapshotRevision = 1;

        memtable.put(new StoredEntry.Value(key("key"), value("newer"), 2));
        memtable.put(new StoredEntry.Value(key("other"), value("value"), 3));

        assertEquals(
            value("older"),
            ((StoredEntry.Value) memtable.get(key("key"), snapshotRevision).orElseThrow()).value()
        );
        assertTrue(memtable.get(key("other"), snapshotRevision).isEmpty());
    }

    @Test
    void capturedViewRetainsImmutableMemtableAfterRetire() {
        MemtableSet memtables = new MemtableSet();
        MutableMemtable active = memtables.active();
        active.put(new StoredEntry.Value(key("key"), value("value"), 1));

        ImmutableMemtable frozen = memtables.rotate(active);
        MemtableView view = memtables.captureView();
        memtables.retire(frozen);

        assertTrue(ReadView.lookupStoredEntry(key("key"), view.activeMemtable(), view.immutableMemtables(), 1).isPresent());
    }
}
