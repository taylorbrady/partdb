package io.partdb.storage;

public interface EntryCursor extends AutoCloseable {

    boolean hasNext();

    VersionedEntry next();

    @Override
    void close();
}
