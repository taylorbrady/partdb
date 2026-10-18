package io.partdb.client;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface Condition {
    Bytes key();

    static Exists exists(Bytes key) {
        return new Exists(key);
    }

    static Missing missing(Bytes key) {
        return new Missing(key);
    }

    static ValueEquals valueEquals(Bytes key, Bytes value) {
        return new ValueEquals(key, value);
    }

    static RevisionEquals revisionEquals(Bytes key, long revision) {
        return new RevisionEquals(key, revision);
    }

    record Exists(Bytes key) implements Condition {
        public Exists {
            Objects.requireNonNull(key, "key must not be null");
        }
    }

    record Missing(Bytes key) implements Condition {
        public Missing {
            Objects.requireNonNull(key, "key must not be null");
        }
    }

    record ValueEquals(Bytes key, Bytes value) implements Condition {
        public ValueEquals {
            Objects.requireNonNull(key, "key must not be null");
            Objects.requireNonNull(value, "value must not be null");
        }
    }

    record RevisionEquals(Bytes key, long revision) implements Condition {
        public RevisionEquals {
            Objects.requireNonNull(key, "key must not be null");
            if (revision <= 0) {
                throw new IllegalArgumentException("revision must be positive");
            }
        }
    }
}
