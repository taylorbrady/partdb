package io.partdb.node.raft;

import io.partdb.bytes.Bytes;

public interface StateMachine {
    void apply(long index, Bytes data);

    Bytes snapshot();

    void restore(long index, Bytes snapshot);
}
