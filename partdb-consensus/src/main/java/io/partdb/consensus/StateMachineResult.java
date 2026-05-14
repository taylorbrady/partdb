package io.partdb.consensus;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface StateMachineResult permits StateMachineResult.Applied, StateMachineResult.Rejected {
    Bytes result();

    record Applied(Bytes result) implements StateMachineResult {
        public Applied {
            result = Objects.requireNonNull(result, "result must not be null");
        }
    }

    record Rejected(Bytes result) implements StateMachineResult {
        public Rejected {
            result = Objects.requireNonNull(result, "result must not be null");
        }
    }
}
