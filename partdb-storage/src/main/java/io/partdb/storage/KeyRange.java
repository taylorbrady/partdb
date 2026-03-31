package io.partdb.storage;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface KeyRange permits KeyRange.All, KeyRange.From, KeyRange.Until, KeyRange.Between {

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

    final class All implements KeyRange {
        private static final All INSTANCE = new All();

        private All() {
        }
    }

    record From(Bytes startInclusive) implements KeyRange {
        public From {
            startInclusive = Objects.requireNonNull(startInclusive, "startInclusive must not be null");
        }
    }

    record Until(Bytes endExclusive) implements KeyRange {
        public Until {
            endExclusive = Objects.requireNonNull(endExclusive, "endExclusive must not be null");
        }
    }

    record Between(Bytes startInclusive, Bytes endExclusive) implements KeyRange {
        public Between {
            startInclusive = Objects.requireNonNull(startInclusive, "startInclusive must not be null");
            endExclusive = Objects.requireNonNull(endExclusive, "endExclusive must not be null");
            if (startInclusive.compareTo(endExclusive) > 0) {
                throw new IllegalArgumentException("startInclusive must be <= endExclusive");
            }
        }
    }
}
