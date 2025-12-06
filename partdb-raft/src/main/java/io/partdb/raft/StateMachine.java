package io.partdb.raft;

public interface StateMachine {
    void apply(long index, byte[] data);

    byte[] snapshot();

    void restore(long index, byte[] snapshot);
}
