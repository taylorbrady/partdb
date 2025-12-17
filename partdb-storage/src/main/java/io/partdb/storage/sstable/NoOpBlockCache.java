package io.partdb.storage.sstable;

public final class NoOpBlockCache implements BlockCache {

    public static final NoOpBlockCache INSTANCE = new NoOpBlockCache();

    private NoOpBlockCache() {}

    @Override
    public Block get(long sstableId, long offset) {
        return null;
    }

    @Override
    public void put(long sstableId, long offset, Block block) {}

    @Override
    public void invalidate(long sstableId) {}

    @Override
    public Stats stats() {
        return new Stats(0, 0, 0, 0, 0);
    }
}
