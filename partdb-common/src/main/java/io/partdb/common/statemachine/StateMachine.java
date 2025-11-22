package io.partdb.common.statemachine;

import io.partdb.common.ByteArray;
import io.partdb.common.Entry;

import java.util.Iterator;
import java.util.Optional;

public interface StateMachine {
    void apply(long index, Operation operation);

    Optional<ByteArray> get(ByteArray key);

    Iterator<Entry> scan(ByteArray startKey, ByteArray endKey);

    StateSnapshot snapshot();

    void restore(StateSnapshot snapshot);

    long lastAppliedIndex();
}
