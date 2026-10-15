package io.partdb.raft.runtime;

import io.partdb.bytes.Bytes;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface StateMachine<R> {
    long appliedIndex();

    R apply(long index, Bytes command);

    void skip(long index);

    long snapshot(OutputStream target) throws IOException;

    void restore(long index, InputStream snapshot) throws IOException;
}
