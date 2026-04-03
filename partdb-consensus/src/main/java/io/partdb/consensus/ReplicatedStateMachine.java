package io.partdb.consensus;

import io.partdb.bytes.Bytes;

public interface ReplicatedStateMachine {
    void apply(long index, Bytes data);

    Bytes snapshot();

    void restore(long index, Bytes snapshot);
}
