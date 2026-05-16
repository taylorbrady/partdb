package io.partdb.storage.internal;

import io.partdb.storage.*;

final class NoOpBlockCache implements BlockCache {

    public static final NoOpBlockCache INSTANCE = new NoOpBlockCache();

    private NoOpBlockCache() {}

    @Override
    public DataBlockReader get(long cacheId, long offset) {
        return null;
    }

    @Override
    public void put(long cacheId, long offset, DataBlockReader block) {}

    @Override
    public void invalidate(long cacheId) {}

    @Override
    public Stats stats() {
        return new Stats(0, 0, 0, 0, 0);
    }
}
