package io.partdb.node.replication;

import io.partdb.bytes.Bytes;
import io.partdb.cluster.ClusterMembership;

import java.util.Objects;

public sealed interface ReplicationLogEntry {
    long index();

    long term();

    record Data(long index, long term, Bytes data) implements ReplicationLogEntry {
        public Data {
            validateIndexAndTerm(index, term);
            data = Objects.requireNonNull(data, "data must not be null");
        }
    }

    record NoOp(long index, long term) implements ReplicationLogEntry {
        public NoOp {
            validateIndexAndTerm(index, term);
        }
    }

    record Config(long index, long term, ClusterMembership membership) implements ReplicationLogEntry {
        public Config {
            validateIndexAndTerm(index, term);
            membership = Objects.requireNonNull(membership, "membership must not be null");
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
