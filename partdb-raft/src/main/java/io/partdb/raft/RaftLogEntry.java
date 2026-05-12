package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface RaftLogEntry {
    long index();
    long term();

    record Data(long index, long term, Bytes data) implements RaftLogEntry {
        public Data {
            if (index < 1) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }

    record NoOp(long index, long term) implements RaftLogEntry {
        public NoOp {
            if (index < 1) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record Config(long index, long term, RaftMembership membership) implements RaftLogEntry {
        public Config {
            if (index < 1) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            if (membership == null) {
                throw new IllegalArgumentException("membership must not be null");
            }
        }
    }
}
