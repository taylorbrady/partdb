package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface LogEntry {
    long index();

    long term();

    default long payloadSize() {
        return switch (this) {
            case Command command -> command.command().size();
            case Configuration _, NoOp _ -> 0;
        };
    }

    record Command(long index, long term, Bytes command) implements LogEntry {
        public Command {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
            Objects.requireNonNull(command, "command must not be null");
        }
    }

    record Configuration(long index, long term, Membership membership) implements LogEntry {
        public Configuration {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
            Objects.requireNonNull(membership, "membership must not be null");
        }
    }

    record NoOp(long index, long term) implements LogEntry {
        public NoOp {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
        }
    }
}
