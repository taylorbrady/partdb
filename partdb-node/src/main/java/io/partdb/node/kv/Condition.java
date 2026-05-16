package io.partdb.node.kv;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface Condition permits Condition.Exists, Condition.Missing, Condition.ValueEquals, Condition.RevisionEquals {
    Bytes key();

    static Condition exists(Bytes key) {
        return new Exists(key);
    }

    static Condition missing(Bytes key) {
        return new Missing(key);
    }

    static Condition valueEquals(Bytes key, Bytes value) {
        return new ValueEquals(key, value);
    }

    static Condition revisionEquals(Bytes key, long revision) {
        return new RevisionEquals(key, revision);
    }

    record Exists(Bytes key) implements Condition {
        public Exists {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }

    record Missing(Bytes key) implements Condition {
        public Missing {
            key = Objects.requireNonNull(key, "key must not be null");
        }
    }

    record ValueEquals(Bytes key, Bytes value) implements Condition {
        public ValueEquals {
            key = Objects.requireNonNull(key, "key must not be null");
            value = Objects.requireNonNull(value, "value must not be null");
        }
    }

    record RevisionEquals(Bytes key, long revision) implements Condition {
        public RevisionEquals {
            key = Objects.requireNonNull(key, "key must not be null");
            if (revision <= 0) {
                throw new IllegalArgumentException("revision must be positive");
            }
        }
    }
}
