package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;

import java.io.IOException;
import java.io.InputStream;

public interface StateMachine {
    long appliedIndex();

    Bytes apply(long index, Bytes command);

    void restore(long index, InputStream snapshot) throws IOException;
}
