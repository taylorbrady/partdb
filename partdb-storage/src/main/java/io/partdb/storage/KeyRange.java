package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface KeyRange {

    static KeyRange all() {
        return All.INSTANCE;
    }

    static KeyRange from(Bytes startInclusive) {
        return new From(startInclusive);
    }

    static KeyRange until(Bytes endExclusive) {
        return new Until(endExclusive);
    }

    static KeyRange between(Bytes startInclusive, Bytes endExclusive) {
        return new Between(startInclusive, endExclusive);
    }

    enum All implements KeyRange {
        INSTANCE
    }

    record From(Bytes startInclusive) implements KeyRange {
        public From {
            Objects.requireNonNull(startInclusive, "startInclusive must not be null");
        }
    }

    record Until(Bytes endExclusive) implements KeyRange {
        public Until {
            Objects.requireNonNull(endExclusive, "endExclusive must not be null");
        }
    }

    record Between(Bytes startInclusive, Bytes endExclusive) implements KeyRange {
        public Between {
            Objects.requireNonNull(startInclusive, "startInclusive must not be null");
            Objects.requireNonNull(endExclusive, "endExclusive must not be null");
            if (startInclusive.compareTo(endExclusive) > 0) {
                throw new IllegalArgumentException("startInclusive must be <= endExclusive");
            }
        }
    }
}
