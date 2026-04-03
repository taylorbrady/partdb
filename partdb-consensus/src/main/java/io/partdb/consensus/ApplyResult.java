package io.partdb.consensus;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface ApplyResult permits ApplyResult.Applied, ApplyResult.Rejected {
    Bytes result();

    record Applied(Bytes result) implements ApplyResult {
        public Applied {
            result = Objects.requireNonNull(result, "result must not be null");
        }
    }

    record Rejected(Bytes result) implements ApplyResult {
        public Rejected {
            result = Objects.requireNonNull(result, "result must not be null");
        }
    }
}
