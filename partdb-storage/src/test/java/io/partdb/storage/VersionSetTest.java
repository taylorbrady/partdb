package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VersionSetTest {

    @Test
    void retiredVersionDrainsAfterLastLeaseDrains() {
        StoreVersion version = new StoreVersion(emptyManifest(), List.of());
        StoreVersion.Lease lease = version.tryAcquire();

        assertNotNull(lease);

        version.retire(VersionRetirement.none());

        lease.close();

        assertTrue(version.awaitDrain(Duration.ofSeconds(1)));
    }

    @Test
    void installRetiresPreviousVersionWithoutBlockingCurrentVersionSet() {
        StoreVersion initial = new StoreVersion(emptyManifest(), List.of());
        VersionSet versionSet = new VersionSet(initial);
        VersionLease snapshot = versionSet.acquire(3);

        versionSet.install(
            new StoreVersion(new SSTableManifest(1, 0, List.of()), List.of()),
            VersionRetirement.none()
        );

        try (VersionLease current = versionSet.acquire(5)) {
            assertTrue(current.manifest().nextSSTableId() == 1);
        }

        snapshot.close();

        assertTrue(initial.awaitDrain(Duration.ofSeconds(1)));
    }

    @Test
    void tracksOldestOpenSnapshotRevision() {
        VersionSet versionSet = new VersionSet(new StoreVersion(emptyManifest(), List.of()));

        assertTrue(versionSet.oldestSnapshotRevision() == Long.MAX_VALUE);

        try (VersionLease newer = versionSet.acquire(8);
             VersionLease older = versionSet.acquire(3)) {
            assertTrue(versionSet.oldestSnapshotRevision() == 3);
        }

        assertTrue(versionSet.oldestSnapshotRevision() == Long.MAX_VALUE);
    }

    private static SSTableManifest emptyManifest() {
        return new SSTableManifest(0, 0, List.of());
    }
}
