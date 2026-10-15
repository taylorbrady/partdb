package io.partdb.raft;

import io.partdb.bytes.Bytes;

import java.util.Objects;

public sealed interface RaftLogEntry {
    long index();

    long term();

    default long payloadSize() {
        return switch (this) {
            case Command command -> command.command().size();
            case Membership _, NoOp _ -> 0;
        };
    }

    record Command(long index, long term, Bytes command) implements RaftLogEntry {
        public Command {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
            Objects.requireNonNull(command, "command must not be null");
        }
    }

    record Membership(long index, long term, RaftMembership membership) implements RaftLogEntry {
        public Membership {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
            Objects.requireNonNull(membership, "membership must not be null");
        }
    }

    record NoOp(long index, long term) implements RaftLogEntry {
        public NoOp {
            Validation.positive(index, "index");
            Validation.positive(term, "term");
        }
    }
}
