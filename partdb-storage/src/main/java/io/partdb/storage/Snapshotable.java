package io.partdb.storage;

public interface Snapshotable {

    byte[] toSnapshot(long checkpoint);

    long restoreSnapshot(byte[] data);
}
