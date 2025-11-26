package io.partdb.common.statemachine;

import io.partdb.common.ByteArray;
import io.partdb.common.CloseableIterator;
import io.partdb.common.Entry;

import java.util.Optional;

public interface StateMachine {
    void apply(long index, Operation operation);

    Optional<ByteArray> get(ByteArray key);

    CloseableIterator<Entry> scan(ByteArray startKey, ByteArray endKey);

    StateSnapshot snapshot();

    void restore(StateSnapshot snapshot);

    long lastAppliedIndex();
}
