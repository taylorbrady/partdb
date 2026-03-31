package io.partdb.storage;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

final class CatalogManager {

    private final ReentrantLock changeLock;
    private final Condition generationChanged;

    private volatile CatalogGeneration currentGeneration;
    private volatile boolean closed;

    CatalogManager(CatalogGeneration initialGeneration) {
        this.currentGeneration = Objects.requireNonNull(initialGeneration, "initialGeneration");
        this.changeLock = new ReentrantLock();
        this.generationChanged = changeLock.newCondition();
    }

    CatalogSnapshot acquire() {
        while (true) {
            CatalogGeneration generation = currentGeneration;
            if (closed) {
                throw new StorageException.Closed("Catalog is closed");
            }

            CatalogGeneration.CatalogLease lease = generation.tryAcquire();
            if (lease != null) {
                return new CatalogSnapshot(lease);
            }

            changeLock.lock();
            try {
                while (!closed && currentGeneration == generation && generation.isRetired()) {
                    try {
                        generationChanged.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new StorageException.IO("Interrupted waiting for catalog generation", e);
                    }
                }
            } finally {
                changeLock.unlock();
            }
        }
    }

    SSTableManifest manifest() {
        return currentGeneration.manifest();
    }

    CatalogGeneration currentGeneration() {
        return currentGeneration;
    }

    void install(CatalogGeneration nextGeneration, List<Runnable> cleanupActions) {
        Objects.requireNonNull(nextGeneration, "nextGeneration");
        Objects.requireNonNull(cleanupActions, "cleanupActions");

        changeLock.lock();
        try {
            ensureOpen();
            CatalogGeneration previous = currentGeneration;
            currentGeneration = nextGeneration;
            previous.retire(cleanupActions);
            generationChanged.signalAll();
        } finally {
            changeLock.unlock();
        }
    }

    CatalogGeneration retireCurrent(List<Runnable> cleanupActions) {
        Objects.requireNonNull(cleanupActions, "cleanupActions");

        changeLock.lock();
        try {
            ensureOpen();
            CatalogGeneration generation = currentGeneration;
            generation.retire(cleanupActions);
            return generation;
        } finally {
            changeLock.unlock();
        }
    }

    void replaceCurrent(CatalogGeneration nextGeneration) {
        Objects.requireNonNull(nextGeneration, "nextGeneration");

        changeLock.lock();
        try {
            ensureOpen();
            currentGeneration = nextGeneration;
            generationChanged.signalAll();
        } finally {
            changeLock.unlock();
        }
    }

    CatalogGeneration close() {
        changeLock.lock();
        try {
            closed = true;
            generationChanged.signalAll();
            return currentGeneration;
        } finally {
            changeLock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException.Closed("Catalog is closed");
        }
    }
}
