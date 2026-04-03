package io.partdb.consensus;

import io.partdb.bytes.Bytes;

public interface ReplicatedStateMachine {
    ApplyResult apply(long index, Bytes data);

    Bytes snapshot();

    void restore(long index, Bytes snapshot);
}
