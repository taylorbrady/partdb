package io.partdb.storage.sstable;

import java.util.List;

public interface ReadSet extends AutoCloseable {

    List<SSTable> level0();

    List<SSTable> level(int level);

    int maxLevel();

    List<SSTable> all();

    @Override
    void close();
}
