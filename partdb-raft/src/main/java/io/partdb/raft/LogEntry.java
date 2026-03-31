package io.partdb.raft;

import java.util.Objects;

public sealed interface LogEntry {
    long index();
    long term();

    record Data(long index, long term, byte[] data) implements LogEntry {
        public Data {
            if (index < 1) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
            Objects.requireNonNull(data, "data must not be null");
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    record NoOp(long index, long term) implements LogEntry {
        public NoOp {
            if (index < 1) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must be non-negative");
            }
        }
    }

    record Config(long index, long term, RaftMembership membership) implements LogEntry {
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
