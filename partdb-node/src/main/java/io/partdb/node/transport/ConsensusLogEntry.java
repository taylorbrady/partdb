package io.partdb.node.transport;

import io.partdb.node.NodeMembership;

import java.util.Objects;

public sealed interface ConsensusLogEntry {
    long index();

    long term();

    record Data(long index, long term, byte[] data) implements ConsensusLogEntry {
        public Data {
            validateIndexAndTerm(index, term);
            Objects.requireNonNull(data, "data must not be null");
            data = data.clone();
        }

        @Override
        public byte[] data() {
            return data.clone();
        }
    }

    record NoOp(long index, long term) implements ConsensusLogEntry {
        public NoOp {
            validateIndexAndTerm(index, term);
        }
    }

    record Config(long index, long term, NodeMembership membership) implements ConsensusLogEntry {
        public Config {
            validateIndexAndTerm(index, term);
            Objects.requireNonNull(membership, "membership must not be null");
        }
    }

    private static void validateIndexAndTerm(long index, long term) {
        if (index < 1) {
            throw new IllegalArgumentException("index must be positive");
        }
        if (term < 0) {
            throw new IllegalArgumentException("term must be non-negative");
        }
    }
}
