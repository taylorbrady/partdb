package io.partdb.storage;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogManagerTest {

    @Test
    void retiredGenerationCleansUpAfterLastLeaseDrains() {
        CatalogGeneration generation = new CatalogGeneration(emptyManifest(), List.of());
        CatalogGeneration.CatalogLease lease = generation.tryAcquire();
        AtomicBoolean cleaned = new AtomicBoolean(false);

        assertNotNull(lease);

        generation.retire(List.of(() -> cleaned.set(true)));

        assertFalse(cleaned.get());

        lease.close();

        assertTrue(generation.awaitDrain(Duration.ofSeconds(1)));
        assertTrue(cleaned.get());
    }

    @Test
    void installRetiresPreviousGenerationWithoutBlockingCurrentCatalog() {
        CatalogGeneration initial = new CatalogGeneration(emptyManifest(), List.of());
        CatalogManager manager = new CatalogManager(initial);
        CatalogSnapshot snapshot = manager.acquire();
        AtomicBoolean cleaned = new AtomicBoolean(false);

        manager.install(
            new CatalogGeneration(new SSTableManifest(1, List.of()), List.of()),
            List.of(() -> cleaned.set(true))
        );

        assertFalse(cleaned.get());

        try (CatalogSnapshot current = manager.acquire()) {
            assertTrue(current.manifest().nextSSTableId() == 1);
        }

        snapshot.close();

        assertTrue(initial.awaitDrain(Duration.ofSeconds(1)));
        assertTrue(cleaned.get());
    }

    private static SSTableManifest emptyManifest() {
        return new SSTableManifest(0, List.of());
    }
}
