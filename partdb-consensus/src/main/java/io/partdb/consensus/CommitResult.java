package io.partdb.consensus;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface CommitResult permits CommitResult.Applied, CommitResult.Rejected {
    long index();

    long term();

    Bytes result();

    record Applied(long index, long term, Bytes result) implements CommitResult {
        public Applied {
            if (index <= 0) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must not be negative");
            }
            result = Objects.requireNonNull(result, "result must not be null");
        }
    }

    record Rejected(long index, long term, Bytes result) implements CommitResult {
        public Rejected {
            if (index <= 0) {
                throw new IllegalArgumentException("index must be positive");
            }
            if (term < 0) {
                throw new IllegalArgumentException("term must not be negative");
            }
            result = Objects.requireNonNull(result, "result must not be null");
        }
    }
}
